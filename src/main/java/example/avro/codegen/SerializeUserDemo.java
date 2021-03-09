package example.avro.codegen;

import example.avro.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SerializeUserDemo {

  public static void main(String[] args) throws IOException {
    User user1 = new User();
    user1.setName("Alyssa");
    user1.setFavoriteNumber(256);
    // Leave favorite color null

    // Alternate constructor
    User user2 = new User("Ben", 7, "red");

    // Construct via builder
    User user3 =
        User.newBuilder()
            .setName("Charlie")
            .setFavoriteColor("blue")
            .setFavoriteNumber(null)
            .build();

    // Serialize user1, user2 and user3 to disk
    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
    DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
    File dataFile = new File("avro/destination/users.avro");
    dataFileWriter.create(user1.getSchema(), dataFile);
    dataFileWriter.append(user1);
    dataFileWriter.append(user2);
    dataFileWriter.append(user3);
    dataFileWriter.close();
  }
}
