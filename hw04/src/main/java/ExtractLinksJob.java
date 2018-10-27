import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class ExtractLinksJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
        // Temporary solution for fast debugging
        FileUtils.deleteDirectory(new File("output"));

        int exitCode = ToolRunner.run(new ExtractLinksJob(), args);
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ExtractLinksJob.class);
        job.setJobName(ExtractLinksJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(ExtractLinksMapper.class);
        job.setReducerClass(ExtractLinksReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1]);

        Configuration configuration = job.getConfiguration();
        configuration.set("urlPath", args[2]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ExtractLinksMapper extends Mapper<Text, Text, Text, Text>{
        HashMap<String, String> idxToURI = new HashMap<>();
        HashMap<String, String> URIToidx = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Pre-calculate idx-to-URI and URI-to-idx dictionaries
            Configuration configuration = context.getConfiguration();
            Path path = new Path(configuration.get("urlPath"));
            FileSystem fs = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            while((line = bufferedReader.readLine()) != null){
                String[] strings = line.split("\\t");
                URI uri = URI.create(strings[1]);
                idxToURI.put(strings[0], "http://" + uri.getHost() + uri.getPath());
                URIToidx.put("http://" + uri.getHost() + uri.getPath(), strings[0]);
            }
        }

        @Override
        protected void map(Text nodeId, Text HTMLEncoded, Context context) throws IOException, InterruptedException {
            LinksExtractor linksExtractor = new LinksExtractor(HTMLEncoded.toString(), idxToURI.get(nodeId.toString()));
            String link, outId;
            while (!linksExtractor.finished()){
                link = linksExtractor.getNextLink();
                if(URIToidx.containsKey(link)) {
                    outId = URIToidx.get(link);
                    // Ignore self links
                    if(nodeId.toString().equals(outId)){
                        continue;
                    }
                    // Emit edge: cur_node -> out_node
                    context.write(nodeId, new Text(outId + ">"));
                    // Emit edge: out_node <- cur_node
                    context.write(new Text(outId), new Text(nodeId.toString() + "<"));
                }
            }
        }
    }

    public static class ExtractLinksReducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text nodeId, Iterable<Text> edges, Context context) throws IOException, InterruptedException {
            // Line format: ID\tID, ID, ... , ID, -1, ID, ... , ID
            //                 |    outEdges    |   |   inEdges   |
            StringBuilder inEdges = new StringBuilder(), outEdges = new StringBuilder();
            for(Text edge : edges){
                String edgeId = edge.toString();
                if(edgeId.contains(">")) {
                    outEdges.append(edgeId.substring(0, edgeId.length() - 1)).append(',');
                } else {
                    inEdges.append(edgeId.substring(0, edgeId.length() - 1)).append(',');
                }
            }
            String edgesLine = outEdges + "-1," + inEdges;
            edgesLine = edgesLine.substring(0, edgesLine.length() - 1);
            context.write(nodeId, new Text(edgesLine));
        }
    }
}
