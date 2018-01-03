package org.gbif.pipelines.core.functions;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.Map;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.UntypedOccurrence;

/**
 * A builder of UntypedOccurrences which uses introspection to locate all suitable terms (e.g. Darwin Core) from the
 * source records.
 */
class UntypedOccurrenceBuilder implements SerializableFunction<ExtendedRecord, UntypedOccurrence> {

  @Override
  public UntypedOccurrence apply(ExtendedRecord record) {
    UntypedOccurrence parsed = new UntypedOccurrence();
    parsed.setOccurrenceId(record.getId());

    // rewrite only to enable lookup by String
    Map<String,String> termsAsString = new HashMap<>();
    record.getCoreTerms().forEach((k,v)-> termsAsString.put(k.toString(), v.toString()));

    // set all DwC fields on the UntypedOccurrence
    try {
      BeanInfo info = Introspector.getBeanInfo(UntypedOccurrence.class);
      for (PropertyDescriptor pd : info.getPropertyDescriptors()) {
        if (pd.getWriteMethod() != null) {
          String term = pd.getName();
          String value = termsAsString.get(DwcTerm.NS + term);
          if (value != null) {
            pd.getWriteMethod().invoke(parsed, value);
          }
        }
      }
      return parsed;
    } catch (Exception e) {
      throw new RuntimeException("Unable to introspect UntypedOccurrence", e);

    }
  }
}
