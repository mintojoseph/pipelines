package au.org.ala.pipelines.interpreters;

import static org.gbif.pipelines.parsers.utils.ModelUtils.addIssue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.extractValue;
import static org.gbif.pipelines.parsers.utils.ModelUtils.hasValue;

import au.org.ala.pipelines.parser.DateParser;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.TemporalAccessor;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.core.ParseResult.CONFIDENCE;
import org.gbif.common.parsers.date.TemporalAccessorUtils;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.core.interpreters.core.TemporalInterpreter;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

public class ALATemporalInterpreter {

  protected static final LocalDate MIN_LOCAL_DATE = LocalDate.of(1600, 1, 1);

  /** Inherit from GBIF interpretTemporal method. Add extra assertions */
  public static void interpretTemporal(ExtendedRecord er, TemporalRecord tr) {
    TemporalInterpreter.interpretTemporal(er, tr);
    tryDMYFormat(er, tr);
    checkRecordDateQuality(er, tr);
    checkDateIdentified(tr);
    checkGeoreferencedDate(er, tr);
  }

  /**
   * If interpret eventdate/dateIdentified failed, parse date with D/M/Y format
   *
   * @param er
   * @param tr
   */
  private static void tryDMYFormat(ExtendedRecord er, TemporalRecord tr) {
    if (StringUtils.isNotBlank(extractValue(er, DwcTerm.eventDate))
        && tr.getIssues().getIssueList().contains(OccurrenceIssue.RECORDED_DATE_INVALID.name())) {

      ParseResult<TemporalAccessor> eventDate =
          DateParser.parse(extractValue(er, DwcTerm.eventDate));
      if (eventDate.isSuccessful()) {
        LocalDateTime ldt =
            TemporalAccessorUtils.toEarliestLocalDateTime(eventDate.getPayload(), true);

        Optional.ofNullable(ldt)
            .map(LocalDateTime::toString)
            .ifPresent(
                (x) -> {
                  tr.setEventDate(new EventDate(x, null));
                  if (eventDate.getConfidence() == CONFIDENCE.POSSIBLE) {
                    tr.getIssues()
                        .getIssueList()
                        .add(ALAOccurrenceIssue.EVENTDATE_ASSUMED_DMY_FORMAT.name());
                  }
                });
      }
    }

    if (StringUtils.isNotBlank(extractValue(er, DwcTerm.dateIdentified))
        && StringUtils.isBlank(tr.getDateIdentified())) {

      ParseResult<TemporalAccessor> dataIdentified =
          DateParser.parse(extractValue(er, DwcTerm.dateIdentified));
      if (dataIdentified.isSuccessful()) {
        LocalDateTime ldt =
            TemporalAccessorUtils.toEarliestLocalDateTime(dataIdentified.getPayload(), true);
        Optional.ofNullable(ldt)
            .map(LocalDateTime::toString)
            .ifPresent(
                (x) -> {
                  tr.setDateIdentified(x);
                });
      }
    }
  }

  /**
   * Code copied from GBIF. Add an extra assertion
   *
   * <p>Raise Missing_COLLECTION_DATE ASSERTION
   */
  private static void checkRecordDateQuality(ExtendedRecord er, TemporalRecord tr) {
    final String year = extractValue(er, DwcTerm.year);
    final String month = extractValue(er, DwcTerm.month);
    final String day = extractValue(er, DwcTerm.day);
    final String dateString = extractValue(er, DwcTerm.eventDate);
    boolean atomizedDateProvided =
        StringUtils.isNotBlank(year)
            || StringUtils.isNotBlank(month)
            || StringUtils.isNotBlank(day);
    boolean dateStringProvided = StringUtils.isNotBlank(dateString);

    if (!atomizedDateProvided && !dateStringProvided) {
      addIssue(tr, ALAOccurrenceIssue.MISSING_COLLECTION_DATE.name());
    }

    if (tr.getDay() != null && tr.getDay() == 1) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_MONTH.name());
    }
    if (tr.getMonth() != null && tr.getMonth() == 1) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_YEAR.name());
    }
    if (tr.getYear() != null && tr.getYear() % 100 == 0) {
      addIssue(tr, ALAOccurrenceIssue.FIRST_OF_CENTURY.name());
    }
  }

  /** All verification process require TemporalInterpreter.interpretTemporal has been called. */
  private static void checkDateIdentified(TemporalRecord tr) {
    if (tr.getEventDate() != null && StringUtils.isNotBlank(tr.getDateIdentified())) {

      ParseResult<TemporalAccessor> parsedIdentifiedResult =
          DateParser.parse(tr.getDateIdentified());
      ParseResult<TemporalAccessor> parsedEventDateResult =
          DateParser.parse(tr.getEventDate().getGte());

      if (parsedEventDateResult.isSuccessful() && parsedIdentifiedResult.isSuccessful()) {
        if (TemporalAccessorUtils.toDate(parsedEventDateResult.getPayload())
            .after(TemporalAccessorUtils.toDate(parsedIdentifiedResult.getPayload()))) {
          addIssue(tr, ALAOccurrenceIssue.ID_PRE_OCCURRENCE.name());
        }
      }
    }
  }

  /** All verification process require TemporalInterpreter.interpretTemporal has been called. */
  private static void checkGeoreferencedDate(ExtendedRecord er, TemporalRecord tr) {

    if (tr.getEventDate() != null && hasValue(er, DwcTerm.georeferencedDate)) {
      ParseResult<TemporalAccessor> parsedGeoreferencedResult =
          DateParser.parse(extractValue(er, DwcTerm.georeferencedDate));
      ParseResult<TemporalAccessor> parsedEventDateResult =
          DateParser.parse(tr.getEventDate().getGte());

      if (parsedEventDateResult.isSuccessful() && parsedGeoreferencedResult.isSuccessful()) {
        if (TemporalAccessorUtils.toDate(parsedEventDateResult.getPayload())
            .before(TemporalAccessorUtils.toDate(parsedGeoreferencedResult.getPayload()))) {
          addIssue(tr, ALAOccurrenceIssue.GEOREFERENCE_POST_OCCURRENCE.name());
        }
      }
    }
  }
}
