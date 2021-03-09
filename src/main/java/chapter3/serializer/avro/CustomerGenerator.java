package chapter3.serializer.avro;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public class CustomerGenerator {

    public static Customer getNext() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int id = random.nextInt(0, Integer.MAX_VALUE);
        return Customer.newBuilder()
                .setId(id)
                .setEmail("email" + random.nextInt(0, Integer.MAX_VALUE) + "@example.com")
                .setName("Customer " + id)
                .build();
    }
}
