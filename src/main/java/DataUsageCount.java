import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataUsageCount {
    static class DataMapper extends Mapper<LongWritable, Text, DataFlowType, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] data = StringUtils.split(line, "\t");
            String tel = data[1];
            long upFlow = Long.valueOf(data[data.length - 3]);
            long downFlow = Long.valueOf(data[data.length - 2]);

            Text keyout = new Text(tel);
            DataFlowType valueout = new DataFlowType(tel, upFlow, downFlow);

            context.write( valueout, keyout);
        }
    }

    static class DataReducer extends Reducer<DataFlowType,Text, Text, DataFlowType> {

        protected void reduce(Iterable<DataFlowType> valueout,Text keyout,Context context) throws java.io.IOException ,InterruptedException {
            long up_flow_counter = 0;
            long down_flow_counter = 0;

            for(DataFlowType data : valueout) {
                up_flow_counter += data.getUp_flow();
                down_flow_counter += data.getDn_flow();
            }

            context.write(keyout, (new DataFlowType(keyout.toString(),up_flow_counter,down_flow_counter)));
        }
    }

    public static class DataFlowType implements WritableComparable<DataFlowType> {

        private String tel;//手机号
        private long up_flow;//上行流量
        private long down_flow;//下行流量
        private long sum_flow;//总流量

        //在反序列化时，反射机制需要调用无参构造方法，所以显式定义了一个无参构造方法
        public DataFlowType() {
            super();
        }
        //为了对象数据的初始化方便，加入一个带参的构造函数
        public DataFlowType(String tel, long up_flow, long down_flow) {
            super();
            this.tel = tel;
            this.up_flow = up_flow;
            this.down_flow = down_flow;
            this.sum_flow = up_flow+down_flow;
        }

        // getters and setters
        public String getTel() {
            return tel;
        }

        public void setTel(String tel) {
            this.tel = tel;
        }

        public long getUp_flow() {
            return up_flow;
        }

        public void setUp_flow(long up_flow) {
            this.up_flow = up_flow;
        }

        public long getDn_flow() {
            return down_flow;
        }

        public void setDn_flow(long dn_flow) {
            this.down_flow = dn_flow;
        }

        public long getSum_flow() {
            return sum_flow;
        }

        public void setSum_flow(long sum_flow) {
            this.sum_flow = sum_flow;
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(tel);
            out.writeLong(up_flow);
            out.writeLong(down_flow);
            out.writeLong(sum_flow);
        }

        public void readFields(DataInput in) throws IOException {
            this.tel = in.readUTF();
            this.up_flow = in.readLong();
            this.down_flow = in.readLong();
            this.sum_flow = in.readLong();
        }

        public String toString() {
            return ""+ up_flow+"\t"+down_flow+"\t"+sum_flow;
        }

        @Override
        public int compareTo(DataFlowType o) {
            // TODO Auto-generated method stub
            return this.sum_flow > o.getSum_flow() ? -1:1;
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(DataUsageCount.class);
        job.setNumReduceTasks(1);

        job.setMapperClass(DataMapper.class);
        job.setMapOutputKeyClass(DataFlowType.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DataReducer.class);
        job.setOutputKeyClass(Text.class);    //为job的输出数据设置Key类
        job.setOutputValueClass(DataFlowType.class);   //为job输出设置value类

        FileInputFormat.addInputPath(job, new Path(args[0])); // 表示的需要统计的处理的数据源
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 表示处理过后放在那里

        boolean flag = job.waitForCompletion(true);
        System.exit(flag ? 1 : 0);
    }

}
