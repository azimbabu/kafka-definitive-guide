package example.avro.withoutcodegen;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class SerializeUserDemo {

  public static void main(String[] args) throws IOException {
      File schemaFile = new File(SerializeUserDemo.class.getClassLoader().getResource("avro/schema/user.avsc").getPath());
      Schema schema = new Schema.Parser().parse(schemaFile);

      GenericRecord user1 = new GenericData.Record(schema);
      user1.put("name", "Alyssa");
      user1.put("favorite_number", 256);
      // Leave favorite color null

      GenericRecord user2 = new GenericData.Record(schema);
      user2.put("name", "Ben");
      user2.put("favorite_number", 7);
      user2.put("favorite_color", "red");

      // Serialize user1 and user2 to disk
      File dataFile = new File("avro/destination/users.avro");
      DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
      DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
      dataFileWriter.create(schema, dataFile);
      dataFileWriter.append(user1);
      dataFileWriter.append(user2);
      dataFileWriter.close();
  }
}
