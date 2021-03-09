package example.avro.withoutcodegen;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;

public class DeserializeUserDemo {

  public static void main(String[] args) throws IOException {
    File schemaFile =
        new File(
            SerializeUserDemo.class
                .getClassLoader()
                .getResource("avro/schema/user.avsc")
                .getPath());
    Schema schema = new Schema.Parser().parse(schemaFile);
    File dataFile = new File("avro/destination/users.avro");

    // Deserialize users from disk
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(dataFile, datumReader);
    GenericRecord user = null;
    while (dataFileReader.hasNext()) {
      // Reuse user object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with many items.
      user = dataFileReader.next(user);
      System.out.println(user);
    }
  }
}
