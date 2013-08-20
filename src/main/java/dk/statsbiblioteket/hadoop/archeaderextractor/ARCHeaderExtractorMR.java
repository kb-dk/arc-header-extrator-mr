package dk.statsbiblioteket.hadoop.archeaderextractor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;

public class ARCHeaderExtractorMR extends Configured implements Tool {

    static public class ARCHeaderExtractorMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text tokenValue = new Text();
        private Text tokenKey = new Text();

        @Override
        protected void map(LongWritable offsetKey, Text fileNameValue, Context context) throws IOException, InterruptedException {
            ARCHeaderExtractor extractHeaders = new ARCHeaderExtractor();
            StringWriter headersString = new StringWriter();

            try {
                Map<Long, String> headers = extractHeaders.extract(fileNameValue.toString());
                if (headers != null) {

                    for (Map.Entry<Long, String> entry : headers.entrySet()) {
                        Long key = entry.getKey();
                        String value = entry.getValue();

                        if (value.length() != 0) {
                            headersString.write("Filename: " + fileNameValue.toString() + "\n");
                            headersString.write(value);
                        }

                    }

                } else {
                    System.err.println("No records were found in the ARC file!");
                }

                tokenValue.set(headersString.toString());
                tokenKey.set(fileNameValue.toString());

                context.write(tokenKey, tokenValue);
            } catch (FileNotFoundException e) {
                System.err.println("Not able to open file '" + fileNameValue.toString() + "'.");
            }
        }
    }

    static public class ARCHeaderExtractorReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text fileName, Iterable<Text> headers, Context context)
                throws IOException, InterruptedException {


            Text allHeaders = new Text();

            for (Text header : headers)
                allHeaders.set(header);

            context.write(fileName, allHeaders);
        }
    }

    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        Job job = new Job(configuration, "ARC Header Extractor");
        job.setJarByClass(ARCHeaderExtractorMR.class);

        job.setMapperClass(ARCHeaderExtractorMapper.class);
        job.setCombinerClass(ARCHeaderExtractorReducer.class);
        job.setReducerClass(ARCHeaderExtractorReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        int n = args.length;
        if (n == 0 || n > 2) {
            System.err.println("Not enough arguments. input dir and output dir mandatory. Only " + n + " were supplied.");
            System.exit(0);
        }

        SequenceFileInputFormat.addInputPath(job, new Path(args[0]));
        SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new ARCHeaderExtractorMR(), args));
    }
}
