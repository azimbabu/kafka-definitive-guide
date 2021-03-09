package baeldung.avro;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.Collections;

public class SchemaBuilderDemo {

  public static void main(String[] args) {
      Schema clientIdentifierSchema = SchemaBuilder.record("ClientIdentifier")
              .namespace("baeldung.avro")
              .fields()
              .requiredString("hostName")
              .requiredString("ipAddress")
              .endRecord();

      Schema avroHttpRequestSchema = SchemaBuilder.record("AvroHttpRequest")
              .namespace("baeldung.avro")
              .fields()
              .requiredLong("requestTime")
              .name("clientIdentifier").type(clientIdentifierSchema).noDefault()
              .name("employeeNames").type().array().items().stringType().arrayDefault(Collections.emptyList())
              .name("active").type().enumeration("Active").symbols("YES", "NO").noDefault()
              .endRecord();

      System.out.println(avroHttpRequestSchema.toString());
  }
}
