import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.NullWriter;
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

public class GenerateURLSJob extends Configured implements Tool {
    public static void main(String[] args) throws Exception{
//         Temporary solution for fast debugging
        FileUtils.deleteDirectory(new File("output"));

        int exitCode = ToolRunner.run(new GenerateURLSJob(), args);
        System.exit(exitCode);
    }

    private Job GetJobConf(Configuration configuration, String input, String outDir) throws IOException {
        Job job = Job.getInstance(configuration);
        job.setJarByClass(GenerateURLSJob.class);
        job.setJobName(GenerateURLSJob.class.getCanonicalName());

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outDir));

        job.setMapperClass(GenerateURLSMapper.class);
        job.setReducerClass(GenerateURLSReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{
        Job job = GetJobConf(getConf(), args[0], args[1]);

        Configuration configuration = job.getConfiguration();
        configuration.set("urlPath", args[2]);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class GenerateURLSMapper extends Mapper<Text, Text, Text, LongWritable>{
        HashMap<String, String> URIToidx = new HashMap<>();
        HashMap<String, String> idxToURI = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // Pre-calculate idx-to-URI and URI-to-idx dictionaries
            Configuration configuration = context.getConfiguration();
            Path path = new Path(configuration.get("urlPath"));
            FileSystem fs = path.getFileSystem(configuration);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line, URIHost, URIPath;
            while((line = bufferedReader.readLine()) != null){
                String[] strings = line.split("\\t");
                URI uri = URI.create(strings[1]);
                URIHost = uri.getHost().replace("\"", "");
                URIPath = uri.getPath().replace("\"", "");
                URIToidx.put("http://" + URIHost + path, strings[0]);
                idxToURI.put(strings[0], "http://" + URIHost + URIPath);
            }
        }

        @Override
        protected void map(Text nodeId, Text HTMLEncoded, Context context) throws IOException, InterruptedException {
            LinksExtractor linksExtractor = new LinksExtractor(HTMLEncoded.toString(), idxToURI.get(nodeId.toString()));
            String link;
            long idx;
            while (!linksExtractor.finished()){
                link = linksExtractor.getNextLink();
                if(URIToidx.containsKey(link)) {
                    idx = Long.valueOf(URIToidx.get(link));
                } else {
                    idx = -1;
                }
                context.write(new Text(link), new LongWritable(idx));
            }
            context.write(
                    new Text(idxToURI.get(nodeId.toString())),
                    new LongWritable(Long.valueOf(nodeId.toString()))
            );
        }
    }

    public static class GenerateURLSReducer extends Reducer<Text, LongWritable, LongWritable, Text>{
        long idx_bias = 564549;

        @Override
        protected void reduce(Text url, Iterable<LongWritable> idxs, Context context) throws IOException, InterruptedException {
            long idx = 1_000_000_000, tmp_idx;
            for(LongWritable value : idxs){
                tmp_idx = Long.valueOf(value.toString());
                if(tmp_idx < idx){
                    idx = tmp_idx;
                }
            }
            if(idx == -1) {
                context.write(new LongWritable(idx_bias), new Text(url));
                idx_bias += 1;
            } else {
                context.write(new LongWritable(idx), new Text(url));
            }
        }
    }
}
