import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.beam.sdk.io.TextIO;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class AvroToBigTable
{
	private static final byte[] FAMILY = Bytes.toBytes("csv");
	private static final Logger LOG = LoggerFactory.getLogger(AvroToBigTable.class);

	static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
		@ProcessElement
		public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
			try {
				String[] headers = c.getPipelineOptions().as(BigtableCsvOptions.class).getHeaders()
									.split(",");
				String[] values = c.element().split(",");
				Preconditions.checkArgument(headers.length == values.length);

				byte[] rowkey = Bytes.toBytes(values[0]);
				byte[][] headerBytes = new byte[headers.length][];
				for (int i = 0; i < headers.length; i++) {
					headerBytes[i] = Bytes.toBytes(headers[i]);
				}

				Put row = new Put(rowkey);
				long timestamp = System.currentTimeMillis();
				for (int i = 1; i < values.length; i++) {
					row.addColumn(FAMILY, headerBytes[i], timestamp, Bytes.toBytes(values[i]));
				}
				c.output(row);
			} catch (Exception e) {
				LOG.error("Failed to process input {}", c.element(), e);
				throw e;
			}

		}
	};


	public static interface BigtableCsvOptions extends CloudBigTableOptions {

		@Description("The headers for the avro file.")
		String getHeaders();

		void setHeaders(String headers);

		@Description("The Cloud Storage path to the avro file..")
		String getInputFile();

		void setInputFile(String location);

		@Description(
				"Set output required parameter to define output path. A local path and Google Cloud Storage"
						+ " path are both supported.")
		String getOutput();

		void setOutput(String value);

		/**
		 * Set avroSchema required parameter to specify location of the schema. A local path and Google
		 * Cloud Storage path are supported.
		 */
		@Description(
				"Set avroSchema required parameter to specify location of the schema. A local path and "
						+ "Google Cloud Storage path are supported.")
		String getAvroSchema();

		void setAvroSchema(String value);

		@Description(
				"Set avroFile required parameter to specify location of the avro file. A local path and "
						+ "Google Cloud Storage path are supported.")
		String getAvroFile();

		void setAvroFile(String value);
	}

	public static String getSchema(String schemaPath) throws IOException
	{
		ReadableByteChannel chan = FileSystems.open(FileSystems.matchNewResource(
				schemaPath, false));

		try (InputStream stream = Channels.newInputStream(chan)) {
			BufferedReader streamReader = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
			StringBuilder dataBuilder = new StringBuilder();

			String line;
			while ((line = streamReader.readLine()) != null) {
				dataBuilder.append(line);
			}

			return dataBuilder.toString();
		}
	}

	public static void main(String[] args) throws IOException
	{
		BigtableCsvOptions options =
				PipelineOptionsFactory.fromArgs(args).as(BigtableCsvOptions.class);

		options.setAvroSchema("src/main/resources/twitter.avsc");
		options.setAvroFile("src/main/resources/twitter.avro");
		options.setOutput("C:/projects/bigtablepoc/src/main/resources/");
		options.setBigtableProjectId("sample");
		options.setBigtableInstanceId("sample");
		options.setBigtableTableId("sample");

		CloudBigtableTableConfiguration config =
				new CloudBigtableTableConfiguration.Builder()
						.withProjectId(options.getBigtableProjectId())
						.withInstanceId(options.getBigtableInstanceId())
						.withTableId(options.getBigtableTableId())
						.build();

		String schemaJson = getSchema(options.getAvroSchema());
		Schema schema = new Schema.Parser().parse(schemaJson);

		Pipeline p = Pipeline.create(options);

		p.apply("ReadingAvro",AvroIO.readGenericRecords(schema).from(options.getAvroFile()))
		 .apply("convertAvroToString",ParDo.of(new DoFn<GenericRecord, String>()
		 {
		 	@ProcessElement
			 public void convertString(ProcessContext c) throws Exception {
		 		String timestamp = c.element().get("timestamp").toString();
		 		String username = c.element().get("username").toString();
		 		String tweet = c.element().get("tweet").toString();
		 		StringBuffer sb = new StringBuffer();
		 		sb.append(timestamp + "," + username + "," + tweet);
		 		c.output(sb.toString());
			}
		 }))
//		.apply(TextIO.write().to(options.getOutput()).withSuffix(".txt").withoutSharding())
		 .apply("TransformParsingsToBigtable", ParDo.of(MUTATION_TRANSFORM))
		 .apply("WriteToBigtable", CloudBigtableIO.writeToTable(config));

		p.run().waitUntilFinish();
	}
}
