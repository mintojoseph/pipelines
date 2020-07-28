package au.org.ala.parser;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import au.org.ala.pipelines.parser.DateParser;
import java.time.format.DateTimeFormatter;
import org.junit.Test;

public class DateParserTest {
  static DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;

  @Test
  public void testISODates() throws NullPointerException{
    assertEquals("1980-02-01", DateParser.toLocalDate("1980-2-1").format(formatter));
    assertEquals("1996-01-26", DateParser.toLocalDate("1996-01-26T01:00Z").format(formatter));
    assertEquals("1996-01-01", DateParser.toLocalDate("1996-01").format(formatter));
    assertEquals("1996-08-29", DateParser.toLocalDate("1996.08.29").format(formatter));
    assertEquals("1996-08-29", DateParser.toLocalDate("29/AUG/1996").format(formatter));
    assertEquals("1996-08-29", DateParser.toLocalDate("29/8/1996").format(formatter));

    assertTrue(!DateParser.parse("1996.29.08").isSuccessful());
  }

  @Test
  public void testMDYDates() throws NullPointerException{
   assertEquals("1980-02-01T10:10:10", DateParser.parseDMY("01/02/1980 10:10:10").format(DateTimeFormatter.ISO_DATE_TIME));
    assertEquals("1980-02-01", DateParser.parseDMY("1/2/1980 10:10:10").format(formatter));
    assertEquals("1980-02-01", DateParser.parseDMY("01/02/1980T10:10:10").format(formatter));
    assertEquals("1980-11-12", DateParser.parseDMY("12/11/1980T10:10:10").format(formatter));
    assertEquals("1980-02-01", DateParser.parseDMY("1/2/1980T10:10:10").format(formatter));
   assertEquals("1980-02-01", DateParser.parseDMY("01/02/1980").format(formatter));
    assertEquals("1980-02-01", DateParser.parseDMY("1/2/1980").format(formatter));
    assertEquals("1980-02-12", DateParser.parseDMY("12/2/1980").format(formatter));

   assertEquals("1980-02-01", DateParser.parseDMY("02/1980").format(formatter));
   assertEquals("1980-01-01", DateParser.parseDMY("1980").format(formatter));
    assertEquals("1980-02-01", DateParser.parseDMY("2/1980").format(formatter));

  }
}
