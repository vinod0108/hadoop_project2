package org.myorg;
 
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
 public class XMLInputFormat extends TextInputFormat {
    public static final String START_TAG_KEY = "<row>";
    public static final String END_TAG_KEY = "</row>";
 
    /*Creating XMLInputformat Class for reading XML File*/
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new XmlRecordReader();
    }
 
    //static class to read the xml file
    public static class XmlRecordReader extends
            RecordReader<LongWritable, Text> {
        private byte[] startTag;
        private byte[] endTag;
        private long start;
        private long end;
        private FSDataInputStream fsin;
        private DataOutputBuffer buffer = new DataOutputBuffer();
        private LongWritable key = new LongWritable();
        private Text value = new Text();
 
        //initializing the start and the end tag of in the xml file to be parsed and to place the seeking pointer at the start of the file
        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac)
                throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) is;
            String START_TAG_KEY = "<row>";
            String END_TAG_KEY = "</row>";
            startTag = START_TAG_KEY.getBytes("utf-8");
            endTag = END_TAG_KEY.getBytes("utf-8");
 
            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();
 
            FileSystem fs = file.getFileSystem(tac.getConfiguration());
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);
 
        }
 
        // to read the next line(next tag value) from the xml file 
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
 
                            value.set(buffer.getData(), 0, buffer.getLength());
                            key.set(fsin.getPos());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }
 
        @Override
        public LongWritable getCurrentKey() throws IOException,
                InterruptedException {
            return key;
        }
 
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
 
        }
 
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }
 
        @Override
        public void close() throws IOException {
            fsin.close();
        }
 
        //reads until the match xml tag is found 
        private boolean readUntilMatch(byte[] match, boolean withinBlock)
                throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
 
                if (b == -1)
                    return false;
 
                if (withinBlock)
                    buffer.write(b);
 
                if (b == match[i]) {
                    i++;
                    if (i >= match.length)
                        return true;
                } else
                    i = 0;
 
                if (!withinBlock && i == 0 && fsin.getPos() >= end)
                    return false;
            }
        }
 
    }
 
}