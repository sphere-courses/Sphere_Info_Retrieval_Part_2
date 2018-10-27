import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class PageRankGetTopJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new PageRankGetTopJob(), args);
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(PageRankGetTopJob.class);
        job.setJobName(PageRankGetTopJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(PageRankGetTopJob.PageRankGetTopMapper.class);
        job.setReducerClass(PageRankGetTopJob.PageRankGetTopReducer.class);

        job.setOutputKeyClass(DoubleWritable.class);
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
        // idx-to-URL file
        configuration.set("urlPath", args[3]);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class PageRankGetTopMapper extends Mapper<Text, Text, DoubleWritable, Text> {
        @Override
        protected void map(Text nodeInfoText, Text edgesText, Context context) throws IOException, InterruptedException {
            String[] nodeInfo = nodeInfoText.toString().split(",");

            double Pr = Double.valueOf(nodeInfo[1]);

            int nNodes = context.getConfiguration().getInt("nNodes", 1);
            double alpha = context.getConfiguration().getDouble("alpha", 0.);
            double missedPr = context.getConfiguration().getDouble("missedPr", 0.);

            // Fix Page Rank with missing Page Rank
            Pr += (1 - alpha) * missedPr / nNodes;

            context.write(new DoubleWritable(-Pr), new Text(nodeInfo[0]));
        }

    }

    public static class PageRankGetTopReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        HashMap<String, String> idxToURL = new HashMap<>();

        @SuppressWarnings("Duplicates")
        @Override
        protected void setup(Context context) throws IOException {
            // Pre-calculate idx-to-URI dictionary
            Configuration configuration = context.getConfiguration();
            Path path = new Path(configuration.get("urlPath"));
            FileSystem fs = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while((line = bufferedReader.readLine()) != null){
                String[] strings = line.split("\\t");
                idxToURL.put(strings[0], strings[1]);
            }
        }

        @Override
        protected void reduce(DoubleWritable Pr, Iterable<Text> nodesId, Context context) throws IOException, InterruptedException {
            for(Text nodeId : nodesId){
                context.write(Pr, new Text(idxToURL.get(nodeId.toString())));
            }
        }
    }
}
