package baeldung.avro;

import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/** Source : https://www.baeldung.com/java-apache-avro */
public class SerializeDeserializeDemo {

  public static void main(String[] args) {
    ClientIdentifier clientIdentifier =
        ClientIdentifier.newBuilder().setHostName("localhost").setIpAddress("127.0.0.1").build();

    AvroHttpRequest httpRequest =
        AvroHttpRequest.newBuilder()
            .setRequestTime(System.nanoTime())
            .setClientIdentifier(clientIdentifier)
            .setEmployeeNames(Arrays.asList("Azim", "Nitu", "Armina", "Zunairah"))
            .setActive(Active.YES)
            .build();
    System.out.println("Original: " + httpRequest);
    SerializeDeserializeDemo serializeDeserializeDemo = new SerializeDeserializeDemo();
    byte[] data = serializeDeserializeDemo.serializeHttpRequest(httpRequest);
    System.out.println("Data: " + new String(data));
    AvroHttpRequest httpRequestDeserialized = serializeDeserializeDemo.deserializeHttpRequest(data);
    System.out.println("Deserialized: " + httpRequestDeserialized);
  }

  public byte[] serializeHttpRequest(AvroHttpRequest httpRequest) {
    DatumWriter<AvroHttpRequest> datumWriter = new SpecificDatumWriter<>(AvroHttpRequest.class);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    try {
      Encoder jsonEncoder =
          EncoderFactory.get().jsonEncoder(AvroHttpRequest.getClassSchema(), stream);
      datumWriter.write(httpRequest, jsonEncoder);
      jsonEncoder.flush();
      return stream.toByteArray();
    } catch (IOException e) {
      System.err.println("Serialization Error: " + e.getMessage());
      return new byte[0];
    }
  }

  public AvroHttpRequest deserializeHttpRequest(byte[] data) {
    DatumReader<AvroHttpRequest> datumReader = new SpecificDatumReader<>(AvroHttpRequest.class);
    try {
      Decoder decoder =
          DecoderFactory.get().jsonDecoder(AvroHttpRequest.getClassSchema(), new String(data));
      return datumReader.read(null, decoder);
    } catch (IOException e) {
      System.err.println("Deserialization error:" + e.getMessage());
      return null;
    }
  }
}
