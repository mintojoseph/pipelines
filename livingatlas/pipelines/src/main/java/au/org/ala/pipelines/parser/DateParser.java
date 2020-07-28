package au.org.ala.pipelines.parser;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import lombok.NoArgsConstructor;
import org.elasticsearch.common.Strings;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.core.ParseResult.CONFIDENCE;
import org.gbif.common.parsers.date.DateParsers;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.common.parsers.date.TemporalParser;

@NoArgsConstructor
public class DateParser {
  //GBIF date parser, yyyy-mm-dd.
  private static final TemporalParser TEXTDATE_PARSER = DateParsers.defaultTemporalParser();
  //DD/MM/YYYY pattern
  private static DateTimeFormatter DMY_DATE = new DateTimeFormatterBuilder()
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

  public static LocalDateTime parseDMY(String dateString){
     return LocalDateTime.parse(dateString, DMY_DATE);
  }

  public static ParseResult<TemporalAccessor> parse(String dateString){
    ParseResult<TemporalAccessor> parsedDateResultTA;
    //GBIF parser
    if(!Strings.isNullOrEmpty(dateString)){
      parsedDateResultTA =  TEXTDATE_PARSER.parse(dateString);
    }else{
      parsedDateResultTA  = ParseResult.fail();
    }
    //dd/MM/yyyy parser
    if(!parsedDateResultTA.isSuccessful()){
      try{
        LocalDateTime ldt = LocalDateTime.parse(dateString, DMY_DATE);
        if(ldt != null){
          parsedDateResultTA = ParseResult.success(CONFIDENCE.POSSIBLE, LocalDate
              .of(ldt.getYear(), ldt.getMonth(), ldt.getDayOfMonth()));
        }
      }catch (Exception e){

      }
    }
    return parsedDateResultTA;
  }

  public static LocalDateTime toLocalDate(String dateString){
    ParseResult<TemporalAccessor> pr = parse(dateString);
    if(pr.isSuccessful()){
      TemporalAccessor ta = pr.getPayload();
      LocalDateTime ldt = TemporalAccessorUtils.toEarliestLocalDateTime(ta, true);
      return ldt;
    }
    return null;
  }


}
