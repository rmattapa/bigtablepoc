package options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface SampleOptions extends DataflowPipelineOptions {

    /**
     * Set inuptFile required parameter to a file path or glob. A local path and Google Cloud Storage
     * path are both supported.
     */
    @Description(
        "Set inuptFile required parameter to a file path or glob. A local path and Google Cloud "
            + "Storage path are both supported.")
    @Validation.Required
    String getInputFile();

    void setInputFile(String value);

    /**
     * Set output required parameter to define output path. A local path and Google Cloud Storage path
     * are both supported.
     */
    @Description(
        "Set output required parameter to define output path. A local path and Google Cloud Storage"
            + " path are both supported.")
    @Validation.Required
    String getOutput();

    void setOutput(String value);

    /**
     * Set avroSchema required parameter to specify location of the schema. A local path and Google
     * Cloud Storage path are supported.
     */
    @Description(
        "Set avroSchema required parameter to specify location of the schema. A local path and "
            + "Google Cloud Storage path are supported.")
    @Validation.Required
    String getAvroSchema();

    void setAvroSchema(String value);

    /**
     * Set csvDelimiter optional parameter to specify the CSV delimiter. Default delimiter is set to a
     * comma.
     */
    @Description(
        "Set csvDelimiter optional parameter to specify the CSV delimiter. Default delimiter is set"
            + " to a comma.")
    @Default.String(",")
    String getCsvDelimiter();

    void setCsvDelimiter(String delimiter);
}

