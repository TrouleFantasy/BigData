package com.atguigu.mr.kv;


import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
/**
 *
 * <p>
 * Title: GbkOutputFormat
 * </p>
 * <p>
 * 功能描述::
 * hadoop已经限定此输出格式统一为UTF-8，因此为了改变hadoop的输出代码的文本编码只需定义一个和TextOutputFormat相同的类GbkOutputFormat同样继承FileOutputFormat
 * （注意是 org.apache.hadoop.mapreduce.lib.output.FileOutputFormat）
 * </p>
 * <p>
 * Company: adteach
 * </p>
 * @version 1.0
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class GBKFileOutPutFormat<K, V> extends FileOutputFormat<K, V> {
    public static String SEPERATOR = "mapreduce.output.textoutputformat.separator";
    protected static class LineRecordWriter<K, V>
            extends RecordWriter<K, V> {
        private static final String utf8 = "GBK";
        private static final byte[] newline;
        static {
            try {
                newline = "\n".getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        protected DataOutputStream out;
        private final byte[] keyValueSeparator;

        public LineRecordWriter(DataOutputStream out, String keyValueSeparator) {
            this.out = out;
            try {
                this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
            } catch (UnsupportedEncodingException uee) {
                throw new IllegalArgumentException("can't find " + utf8 + " encoding");
            }
        }

        public LineRecordWriter(DataOutputStream out) {
            this(out, "\t");
        }

        /**
         * Write the object to the byte stream, handling Text as a special
         * case.
         * @param o the object to print
         * @throws IOException if the write throws, we pass it on
         */
        private void writeObject(Object o) throws IOException {
            if (o instanceof Text) {
//        Text to = (Text) o;
//        out.write(to.getBytes(), 0, to.getLength());
//      } else {
                out.write(o.toString().getBytes(utf8));
            }
        }

        public synchronized void write(K key, V value)
                throws IOException {

            boolean nullKey = key == null || key instanceof NullWritable;
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullKey && nullValue) {
                return;
            }
            if (!nullKey) {
                writeObject(key);
            }
            if (!(nullKey || nullValue)) {
                out.write(keyValueSeparator);
            }
            if (!nullValue) {
                writeObject(value);
            }
            out.write(newline);
        }

        public synchronized
        void close(TaskAttemptContext context) throws IOException {
            out.close();
        }
    }

    public RecordWriter<K, V>
    getRecordWriter(TaskAttemptContext job
    ) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();
        boolean isCompressed = getCompressOutput(job);
        String keyValueSeparator= conf.get(SEPERATOR, "\t");
        CompressionCodec codec = null;
        String extension = "";
        if (isCompressed) {
            Class<? extends CompressionCodec> codecClass =
                    getOutputCompressorClass(job, GzipCodec.class);
            codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
            extension = codec.getDefaultExtension();
        }
        Path file = getDefaultWorkFile(job, extension);
        FileSystem fs = file.getFileSystem(conf);
        if (!isCompressed) {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
        } else {
            FSDataOutputStream fileOut = fs.create(file, false);
            return new LineRecordWriter<K, V>(new DataOutputStream
                    (codec.createOutputStream(fileOut)),
                    keyValueSeparator);
        }
    }
}
