package au.org.ala.pipelines.parser;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import javax.annotation.Nullable;
import lombok.NoArgsConstructor;
import org.elasticsearch.common.Strings;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.core.ParseResult.CONFIDENCE;
import org.gbif.common.parsers.date.DateFormatHint;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalParser;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Support ISO date formats
 *
 * <p>If date format is ambiguous, e.g 2/3/2000, parse it as dd/MM/YYYY
 */
@NoArgsConstructor
public class DateParser implements TemporalParser {
  // GBIF date parser, yyyy-mm-dd.
  private static final TemporalParser ISO_DATE_PARSER = DateParsers.defaultTemporalParser();
  // DD/MM/YYYY pattern
  private static DateTimeFormatter DMY_DATE =
      new DateTimeFormatterBuilder()
          .appendPattern("[[d/]M/y'T'HH:mm:ss[.SSSSSS]]")
          .appendPattern("[[d/]M/y[ HH:mm:ss[.SSSSSS]]]")
          .appendPattern("[M/y]")
          .appendPattern("[y]")
          .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
          .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
          .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
          .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0)
          .toFormatter();

  public static DateParser getInstance() {
    return new DateParser();
  }

  public LocalDateTime parseDMY(String dateString) {
    return LocalDateTime.parse(dateString, DMY_DATE);
  }

  @Override
  public ParseResult<TemporalAccessor> parse(String dateString) {
    ParseResult<TemporalAccessor> parsedDateResultTA;
    // GBIF parser
    if (!Strings.isNullOrEmpty(dateString)) {
      parsedDateResultTA = ISO_DATE_PARSER.parse(dateString);
    } else {
      parsedDateResultTA = ParseResult.fail();
    }
    // dd/MM/yyyy parser
    if (!parsedDateResultTA.isSuccessful()) {
      try {
        LocalDateTime ldt = LocalDateTime.parse(dateString, DMY_DATE);
        if (ldt != null) {
          parsedDateResultTA =
              ParseResult.success(
                  CONFIDENCE.POSSIBLE,
                  LocalDate.of(ldt.getYear(), ldt.getMonth(), ldt.getDayOfMonth()));
        }
      } catch (Exception e) {

      }
    }
    return parsedDateResultTA;
  }

  public LocalDateTime toLocalDate(String dateString) {
    ParseResult<TemporalAccessor> pr = parse(dateString);
    if (pr.isSuccessful()) {
      TemporalAccessor ta = pr.getPayload();
      LocalDateTime ldt = TemporalAccessorUtils.toEarliestLocalDateTime(ta, true);
      return ldt;
    }
    return null;
  }

  @Override
  public ParseResult<TemporalAccessor> parse(String s, @Nullable DateFormatHint dateFormatHint) {
    throw new NotImplementedException();
  }

  @Override
  public ParseResult<TemporalAccessor> parse(
      @Nullable String s, @Nullable String s1, @Nullable String s2) {
    return ISO_DATE_PARSER.parse(s, s1, s2);
  }

  @Override
  public ParseResult<TemporalAccessor> parse(
      @Nullable Integer integer, @Nullable Integer integer1, @Nullable Integer integer2) {
    return ISO_DATE_PARSER.parse(integer, integer1, integer2);
  }
}
