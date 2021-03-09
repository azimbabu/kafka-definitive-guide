package example.avro.codegen;

import example.avro.User;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

public class DeserializeUserDemo {

  public static void main(String[] args) throws IOException {
    // Deserialize Users from disk
    File dataFile = new File("avro/destination/users.avro");
    DatumReader<User> userDatumReader = new SpecificDatumReader<>(User.class);
    DataFileReader<User> dataFileReader = new DataFileReader<>(dataFile, userDatumReader);
    User user = null;
    while (dataFileReader.hasNext()) {
      // Reuse user object by passing it to next(). This saves us from
      // allocating and garbage collecting many objects for files with many items.
      user = dataFileReader.next(user);
      System.out.println(user);
    }
  }
}
