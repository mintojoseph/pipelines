package org.gbif.xml.occurrence.parser;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ConverterTask;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ExecutorPool;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ParserFileUtils;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.SyncDataFileWriter;
import org.gbif.xml.occurrence.parser.parsing.validators.UniquenessValidator;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import org.apache.avro.file.DataFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parsing xml response files or tar.xz archive and convert to ExtendedRecord avro file
 */
public class ExtendedRecordConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordConverter.class);

  private static final String FILE_PREFIX = ".response";

  private Executor executor = ForkJoinPool.commonPool();

  private ExtendedRecordConverter(Executor executor, Integer parallelism) {
    if (Objects.nonNull(executor)) {
      this.executor = executor;
    } else if (Objects.nonNull(parallelism)) {
      this.executor = ExecutorPool.getInstance(parallelism).getPool();
    }
  }

  public static ExtendedRecordConverter crete(Executor executor) {
    return new ExtendedRecordConverter(executor, null);
  }

  public static ExtendedRecordConverter crete(Integer parallelism) {
    return new ExtendedRecordConverter(null, parallelism);
  }

  public static ExtendedRecordConverter crete() {
    return new ExtendedRecordConverter(null, null);
  }

  /**
   * @param inputPath path to directory with response files or a tar.xz archive
   */
  public void toAvroFromXmlResponse(String inputPath, DataFileWriter<ExtendedRecord> dataFileWriter) {
    if (Strings.isNullOrEmpty(inputPath)) {
      throw new ParsingException("Input or output stream must not be empty or null!");
    }

    File inputFile = ParserFileUtils.uncompressAndGetInputFile(inputPath);

    try (Stream<Path> walk = Files.walk(inputFile.toPath());
         UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      // Class with sync method to avoid problem with writing
      SyncDataFileWriter syncWriter = new SyncDataFileWriter(dataFileWriter);

      // Run async process - read a file, convert to ExtendedRecord and write to avro
      CompletableFuture[] futures = walk.filter(x -> x.toFile().isFile() && x.toString().endsWith(FILE_PREFIX))
        .map(Path::toFile)
        .map(file -> CompletableFuture.runAsync(new ConverterTask(file, syncWriter, validator), executor))
        .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();
      dataFileWriter.flush();

    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new ParsingException(ex);
    }
  }

}