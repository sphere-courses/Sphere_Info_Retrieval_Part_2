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
import java.util.HashSet;

public class HITSGetTopJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new HITSGetTopJob(), args);
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(HITSGetTopJob.class);
        job.setJobName(HITSGetTopJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(HITSGetTopJob.HITSGetTopMapper.class);
        job.setReducerClass(HITSGetTopJob.HITSGetTopReducer.class);

        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1] + '_' + args[2]);

        Configuration configuration = job.getConfiguration();
        // characteristics to sort by
        configuration.set("characteristics", args[2]);
        // idx-to-URL file
        configuration.set("urlPath", args[3]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class HITSGetTopMapper extends Mapper<Text, Text, DoubleWritable, Text> {
        @Override
        protected void map(Text nodeId, Text edgesText, Context context) throws IOException, InterruptedException {
            String[] edgesInfo = edgesText.toString().split(";");

            String[] edgeInfo;
            int pos = context.getConfiguration().get("characteristics").equals("a") ? 2 : 3;
            for(String edge : edgesInfo){
                edgeInfo = edge.split(",");
                context.write(new DoubleWritable(-Double.valueOf(edgeInfo[pos])), new Text(edgeInfo[0]));
            }
        }

    }

    public static class HITSGetTopReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
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
        protected void reduce(DoubleWritable value, Iterable<Text> nodesId, Context context) throws IOException, InterruptedException {
            HashSet<String> uniquer = new HashSet<>();

            for(Text nodeId : nodesId){
                uniquer.add(nodeId.toString());
            }
            for(String nodeId : uniquer){
                try {
                    context.write(value, new Text(idxToURL.get(nodeId)));
                } catch (java.lang.NullPointerException ex){
                    System.out.println(nodeId);
                }
            }
        }
    }
}
