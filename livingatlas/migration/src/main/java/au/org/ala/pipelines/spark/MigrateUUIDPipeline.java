package au.org.ala.pipelines.spark;

import static org.apache.spark.sql.functions.col;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple4;

/**
 * A Spark only pipeline that generates AVRO files for UUIDs based on a CSV export from Cassandra.
 *
 * <p>Previous runs should be cleaned up like so:
 *
 * <p>hdfs dfs -rm -R /pipelines-data/<globstar>/1/identifiers
 */
@Slf4j
@Parameters(separators = "=")
public class MigrateUUIDPipeline implements Serializable {

  @Parameter private List<String> parameters = new ArrayList<>();

  @Parameter(
      names = "--inputPath",
      description =
          "The input path to a CSV export from occ_uuid in cassandra e.g /data/occ_uuid.csv or hdfs://localhost:8020/occ_uuid.csv")
  private String inputPath;

  @Parameter(
      names = "--targetPath",
      description = "The output path e.g /data or hdfs://localhost:8020")
  private String targetPath;

  @Parameter(
      names = "--hdfsSiteConfig",
      description = "The absolute path to a hdfs-site.xml with default.FS configuration")
  private String hdfsSiteConfig;

  public static void main(String[] args) throws Exception {
    MigrateUUIDPipeline m = new MigrateUUIDPipeline();
    JCommander jCommander = JCommander.newBuilder().addObject(m).build();
    jCommander.parse(args);

    if (m.inputPath == null || m.targetPath == null) {
      jCommander.usage();
      System.exit(1);
    }
    m.run();
  }

  private void run() throws Exception {

    FileSystem fileSystem = getFileSystem();
    fileSystem.delete(new Path(targetPath + "/migration-tmp/avro"), true);

    log.info("Starting spark job to migrate UUIDs");
    Schema schemaAvro =
        new Schema.Parser()
            .parse(
                MigrateUUIDPipeline.class
                    .getClassLoader()
                    .getResourceAsStream("ala-uuid-record.avsc"));

    log.info("Starting spark session");
    SparkSession spark = SparkSession.builder().appName("Migration UUIDs").getOrCreate();

    log.info("Load CSV");
    Dataset<Row> dataset = spark.read().csv(inputPath);

    log.info("Load UUIDs");
    Dataset<Tuple4<String, String, String, String>> uuidRecords =
        dataset
            .filter(
                row ->
                    StringUtils.isNotEmpty(row.getString(0))
                        && row.getString(0).startsWith("dr")
                        && row.getString(0).contains("|"))
            .map(
                row -> {
                  String datasetID = row.getString(0).substring(0, row.getString(0).indexOf("|"));
                  return Tuple4.apply(
                      datasetID,
                      "temp_" + datasetID + "_" + row.getString(1),
                      row.getString(1),
                      row.getString(0));
                },
                Encoders.tuple(
                    Encoders.STRING(), Encoders.STRING(), Encoders.STRING(), Encoders.STRING()));

    log.info("Write AVRO");
    uuidRecords
        .select(
            col("_1").as("datasetID"),
            col("_2").as("id"),
            col("_3").as("uuid"),
            col("_4").as("uniqueKey"))
        .write()
        .partitionBy("datasetID")
        .format("avro")
        .option("avroSchema", schemaAvro.toString())
        .mode(SaveMode.Overwrite)
        .save(targetPath + "/migration-tmp/avro");

    Path path = new Path(targetPath + "/migration-tmp/avro/");
    RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
    while (iterator.hasNext()) {

      // datasetID=dr1
      LocatedFileStatus locatedFileStatus = iterator.next();
      Path sourcePath = locatedFileStatus.getPath();
      String fullPath = sourcePath.toString();

      if (fullPath.lastIndexOf("=") > 0) {
        String dataSetID =
            fullPath.substring(fullPath.lastIndexOf("=") + 1, fullPath.lastIndexOf("/"));

        // move to correct location
        String newPath = targetPath + "/" + dataSetID + "/1/identifiers/ala_uuid/";
        fileSystem.mkdirs(new Path(newPath));

        Path destination = new Path(newPath + sourcePath.getName());
        fileSystem.rename(sourcePath, destination);
      }
    }

    log.info("Remove temp directories");
    fileSystem.delete(new Path(targetPath + "/migration-tmp"), true);

    log.info("Close session");
    spark.close();
    log.info("Closed session. Job finished.");
  }

  private FileSystem getFileSystem() throws IOException {
    // move to correct directory structure
    // check if the hdfs-site.xml is provided
    Configuration configuration = new Configuration();
    if (!Strings.isNullOrEmpty(hdfsSiteConfig)) {
      File hdfsSite = new File(hdfsSiteConfig);
      if (hdfsSite.exists() && hdfsSite.isFile()) {
        configuration.addResource(hdfsSite.toURI().toURL());
      }
    }

    // get a list of paths & move to correct directories
    return FileSystem.get(configuration);
  }
}
