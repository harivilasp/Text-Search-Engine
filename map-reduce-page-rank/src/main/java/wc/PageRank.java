package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;


public class PageRank extends Configured implements Tool
{
	private static final Logger logger = LogManager.getLogger(PageRank.class);
	private static final String PREV_DANGLING_MASS = "prevDanglingMass";
	private static final String NO_OF_VERTEX = "noOfVertex";
	private static final long MULTIPLIER = 1000000L;
	private static final double DAMPING_FACTOR = 0.85;
	private static final double BASE_PROB = 1 - DAMPING_FACTOR;

	enum DANGLING_COUNTER {
		DANGLING_MASS, TOTAL_PROB
	}
	public static class MapFirst extends Mapper<Object, Text, Text, Text>
	{
		double prevDanglingMass;
		long noOfVertex;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			prevDanglingMass = Double.parseDouble(conf.get(PREV_DANGLING_MASS)) / MULTIPLIER;
			noOfVertex = Integer.parseInt(conf.get(NO_OF_VERTEX));
		}

		private final Text sourceNodeID = new Text();
		private final Text outputValue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(":");
			String nodeID = parts[0];
			String[] adjacencyList = parts[1].replace("[", "").replace("]", "").split(",");
			double rank = Double.parseDouble(parts[2]) + DAMPING_FACTOR * (prevDanglingMass / (double) noOfVertex);

			sourceNodeID.set(nodeID);
			outputValue.set(parts[1]); // Emit the graph structure
			context.write(sourceNodeID, outputValue);

			for (String destNodeID : adjacencyList) {
				if (!destNodeID.isEmpty() && !nodeID.equalsIgnoreCase("0")) {
					double newPRValue = rank / adjacencyList.length;
					sourceNodeID.set(destNodeID);
					outputValue.set(String.valueOf(newPRValue));
					context.write(sourceNodeID, outputValue);
				}
			}
		}
	}

	public static class ReducerFirst extends Reducer<Text, Text, Text,Text>
	{
		int noOfVertex;

		@Override
		public void setup(Context context) {
			noOfVertex = Integer.parseInt(context.getConfiguration().get(NO_OF_VERTEX));
		}

		private Text outputValue = new Text();

		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			double sumPRValues = 0.0;
			String adjList = "";
			for (Text val : values) {
				String value = val.toString();
				if (value.contains("[")) {
					adjList = value;
				} else {
					sumPRValues += Double.parseDouble(value);
				}
			}
			double newPRValue = BASE_PROB / noOfVertex + DAMPING_FACTOR * sumPRValues;
			if ("0".equals(key.toString())) {
				context.getCounter(DANGLING_COUNTER.DANGLING_MASS).setValue((long) (sumPRValues * MULTIPLIER));
				newPRValue = sumPRValues; // Adjust for dangling node
				adjList = "[]";
			}
			outputValue.set(adjList + ":" + newPRValue);
			context.write(key, outputValue);
		}
	}

	public static class MapLast extends Mapper<Object, Text, Text, Text> {
		double prevDanglingMass;
		int noOfVertex;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			prevDanglingMass = Double.parseDouble(conf.get(PREV_DANGLING_MASS)) / MULTIPLIER;
			noOfVertex = Integer.parseInt(conf.get(NO_OF_VERTEX));
		}

		private final Text sourceNodeID = new Text();
		private final Text finalPRValue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] parts = value.toString().split(":");
			double PRValue = Double.parseDouble(parts[2]);
			String nodeId = parts[0];

			if (!"0".equals(nodeId)) {
				PRValue += DAMPING_FACTOR * (prevDanglingMass / noOfVertex);
//				context.getCounter(DANGLING_COUNTER.TOTAL_PROB).increment((long) (PRValue * MULTIPLIER));
			} else {
				PRValue = 0; // Neutralize dummy node
			}
			sourceNodeID.set(nodeId);
			finalPRValue.set(String.valueOf(PRValue));
			context.write(sourceNodeID, finalPRValue);
		}
	}

	@Override
	public int run(final String[] args) throws Exception
	{
		String baseInputPath = args[0];
		String baseOutputPath = args[1];
		final int k = Integer.parseInt(args[2]);
		int currentIteration = 1;
		final int numIterations = 4;
		double previousDanglingMass = 0.0;

		Job job;
		do {
			String inputPath = currentIteration == 1 ? baseInputPath : baseOutputPath + "/step" + (currentIteration - 1);
			job = createNewPageRankInterationJob(currentIteration, previousDanglingMass, k, inputPath, baseOutputPath);
			job.waitForCompletion(true);
			previousDanglingMass = (double)job.getCounters().findCounter(DANGLING_COUNTER.DANGLING_MASS).getValue();
			currentIteration += 1;
		} while(currentIteration <= numIterations);

		previousDanglingMass = job.getCounters().findCounter(DANGLING_COUNTER.DANGLING_MASS).getValue();
		Configuration confx = getConf();
		job = Job.getInstance(confx, "Page Rank Final");
		job.setJarByClass(PageRank.class);
		Configuration jobConf = job.getConfiguration();
		jobConf.set(PREV_DANGLING_MASS, String.valueOf(previousDanglingMass));
		jobConf.set(NO_OF_VERTEX, String.valueOf(k * k));
		jobConf.set("mapreduce.output.textoutputformat.separator", ",");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		MultipleInputs.addInputPath(job, new Path(baseOutputPath+"/step"+(currentIteration-1)),TextInputFormat.class, MapLast.class);
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path(baseOutputPath+"/step"+currentIteration));
		if(job.waitForCompletion(true))
		{
//			Counter counter2 = job.getCounters().findCounter(DANGLING_COUNTER.TOTAL_PROB);
//			logger.info("Total probability is" + "\t" + (double)(counter2.getValue())/MULTIPLIER);
			return 0;
		}else
		{
			System.out.println("Error in final job");
			return 1;
		}
	}

	public static void main(final String[] args) {
		try {
//			String inputpath = args[0];
			int k = Integer.parseInt(args[2]);
//			ConstructGraph.constructGraph(inputpath, k);
			ToolRunner.run((Tool)new PageRank(), args);
		} catch (final Exception e) {
			logger.error("Error: ", e);
		}
	}

	private Job createNewPageRankInterationJob(int iteration, double previousDanglingMass, int k, String inputPath, String outputPath) throws IOException {
		Configuration conf = getConf();

		String jobName = String.format("Page Rank Iteration %d", iteration);
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(PageRank.class);

		Configuration jobConf = job.getConfiguration();
		// Setting configuration parameters.
		jobConf.set(PREV_DANGLING_MASS, String.valueOf(previousDanglingMass));
		jobConf.set(NO_OF_VERTEX, String.valueOf(k * k));
		jobConf.set("mapreduce.output.textoutputformat.separator", ":");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(NLineInputFormat.class);

		NLineInputFormat.addInputPath(job, new Path(inputPath));
		// This is to ensure at least 21 maps are created.
		int linesPerMap = (k * k) / 21;
		jobConf.setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);

		job.setMapperClass(MapFirst.class);
		job.setReducerClass(ReducerFirst.class);

		String finalOutputPath = String.format("%s/step%d", outputPath, iteration);
		FileOutputFormat.setOutputPath(job, new Path(finalOutputPath));

		return job;
	}

}
