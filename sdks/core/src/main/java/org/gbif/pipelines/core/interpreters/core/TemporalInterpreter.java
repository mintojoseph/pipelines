package org.gbif.pipelines.core.interpreters.core;

import java.time.LocalDate;
import java.time.temporal.TemporalAccessor;

import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

import com.google.common.collect.Range;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static lombok.AccessLevel.PRIVATE;

/** Interprets date representations into a Date to support API v1 */
@Slf4j

@NoArgsConstructor(access = PRIVATE)
public class TemporalInterpreter {

  public static void interpretTemporal(ExtendedRecord er, TemporalRecord tr){
      DefaultTemporalInterpreter.getInstance().interpretTemporal(er, tr);
  }

  /**
   * Given possibly both of year, month, day and a dateString, produces a single date.
   * When year, month and day are all populated and parseable they are given priority,
   * but if any field is missing or illegal and dateString is parseable dateString is preferred.
   * Partially valid dates are not supported and null will be returned instead. The only exception is the year alone
   * which will be used as the last resort if nothing else works.
   * Years are verified to be before or next year and after 1600.
   * x
   *
   * @return interpretation result, never null
   */
  public static  OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(String year, String month, String day,
      String dateString) {

    return  DefaultTemporalInterpreter
        .getInstance().interpretRecordedDate(year,month, day,dateString);
  }

  public static OccurrenceParseResult<TemporalAccessor> interpretRecordedDate(ExtendedRecord er){
      return DefaultTemporalInterpreter.getInstance().interpretRecordedDate(er);
  }

  /**
   * @return TemporalAccessor that represents a LocalDate or LocalDateTime
   */
  public static  OccurrenceParseResult<TemporalAccessor> interpretLocalDate(String dateString,
      Range<LocalDate> likelyRange,
      OccurrenceIssue unlikelyIssue) {

    return DefaultTemporalInterpreter
        .getInstance().interpretLocalDate(dateString, likelyRange, unlikelyIssue);
  }

}
