package Samples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.List;
import options.SampleOptions;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample transformations on PCollections like converting csv files to avro.
 * Transformations on PCollections of Generic avro records.
 */
public class DataTransforms {

    private static final Logger LOG = LoggerFactory.getLogger(DataTransforms.class);
    private static final List<String> acceptedTypes = Arrays.asList(
        new String[]{"string", "boolean", "int", "long", "float", "double"});

    /**
     * Read the avro schema from the given path.
     */
    public static String getSchema(String schemaPath) throws IOException {
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

    /**
     * Method to verify field types in an avro schema.
     */
    public static void checkFieldTypes(Schema schema) throws IllegalArgumentException {
        for (Schema.Field field : schema.getFields()) {
            String fieldType = field.schema().getType().getName().toLowerCase();
            if (!acceptedTypes.contains(fieldType)) {
                LOG.error("Data transformation doesn't support: " + fieldType);
                throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
            }
        }
    }

    /**
     * Method to convert csv files to avro using beam transformations.
     */
    public static void runCsvToAvro(SampleOptions options)
        throws IOException, IllegalArgumentException {
        FileSystems.setDefaultPipelineOptions(options);

        // Get Avro Schema
        String schemaJson = getSchema(options.getAvroSchema());
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Check schema field types before starting the Dataflow job
        checkFieldTypes(schema);

        // Create the Pipeline object with the options we defined above.
        Pipeline pipeline = Pipeline.create(options);

        // Convert CSV to Avro
        pipeline.apply("Read CSV files", TextIO.read().from(options.getInputFile()))
            .apply("Convert CSV to Avro formatted data",
                ParDo.of(new ConvertCsvToAvro(schemaJson, options.getCsvDelimiter())))
            .setCoder(AvroCoder.of(GenericRecord.class, schema))
            .apply("Write Avro formatted data", AvroIO.writeGenericRecords(schemaJson)
                .to(options.getOutput()).withCodec(CodecFactory.snappyCodec()).withSuffix(".avro").withoutSharding());

        // Run the pipeline.
        pipeline.run().waitUntilFinish();
    }

    /**
     * method to convert csv to avro to a batch data.
     */
    public static void transformGenericAvro(SampleOptions options) throws IOException {
        // Create the Pipeline object with the options we defined above.e
        Pipeline pipeline = Pipeline.create(options);

        // Get Avro Schema
        String schemaJson = getSchema(options.getAvroSchema());
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Read GenericRecord's of the given schema from files on GCS
        pipeline.apply("GetAvro", AvroIO.readGenericRecords(schema)
            .from("C:\\Users\\axc353\\Documents\\Git\\indian-premier-league-csv-dataset\\Player.avro"))
            .apply("Batting-style-count", ParDo.of(new DoFn<GenericRecord, KV<String, Integer>>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    String key = c.element().get("Batting_Hand").toString();
                    c.output(KV.of(key, 1));
                }
            }))
            .apply(Sum.integersPerKey())
            .apply("ToString", ParDo.of(new DoFn<KV<String, Integer>, String>() {

                @ProcessElement
                public void processElement(ProcessContext c) throws Exception {
                    StringBuffer sb = new StringBuffer();
                    sb.append(c.element().getKey() + "," + c.element().getValue() + '\n');

                    c.output(sb.toString());
                }
            }))
            .apply(TextIO.write().to(options.getOutput()).withSuffix(".txt").withoutSharding());
        // Run the pipeline.
        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) throws IOException, IllegalArgumentException {
        // Create and set your PipelineOptions.
        SampleOptions options = TestPipeline.testingPipelineOptions().as(SampleOptions.class);
        options.setAvroSchema("resources/user.avsc");
        options.setInputFile("gs://my-test-data-abhishek-1/sample/Player.csv");
        options.setOutput("gs://my-test-data-abhishek-1/sample/Player");

        runCsvToAvro(options);
        transformGenericAvro(options);
    }

    /**
     * class to process csv to avro generic record transformations.
     */
    public static class ConvertCsvToAvro extends DoFn<String, GenericRecord> {

        private String delimiter;
        private String schemaJson;

        public ConvertCsvToAvro(String schemaJson, String delimiter) {
            this.schemaJson = schemaJson;
            this.delimiter = delimiter;
        }

        @ProcessElement
        public void processElement(ProcessContext ctx) throws IllegalArgumentException {
            // Split CSV row into using delimiter
            String[] rowValues = ctx.element().split(delimiter);

            Schema schema = new Schema.Parser().parse(schemaJson);

            // Create Avro Generic Record
            GenericRecord genericRecord = new GenericData.Record(schema);
            List<Schema.Field> fields = schema.getFields();

            for (int index = 0; index < fields.size(); ++index) {
                Schema.Field field = fields.get(index);
                String fieldType = field.schema().getType().getName().toLowerCase();

                switch (fieldType) {
                    case "string":
                        genericRecord.put(field.name(), rowValues[index]);
                        break;
                    case "boolean":
                        genericRecord.put(field.name(), Boolean.valueOf(rowValues[index]));
                        break;
                    case "int":
                        genericRecord.put(field.name(), Integer.valueOf(rowValues[index]));
                        break;
                    case "long":
                        genericRecord.put(field.name(), Long.valueOf(rowValues[index]));
                        break;
                    case "float":
                        genericRecord.put(field.name(), Float.valueOf(rowValues[index]));
                        break;
                    case "double":
                        genericRecord.put(field.name(), Double.valueOf(rowValues[index]));
                        break;
                    default:
                        LOG.error("Data transformation doesn't support: " + fieldType);
                        throw new IllegalArgumentException("Field type " + fieldType + " is not supported.");
                }
            }
            ctx.output(genericRecord);
        }
    }
}

