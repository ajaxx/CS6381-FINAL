import java.time.Duration;
import java.time.Instant;

public class Main {
    public static void main(String[] args) {
        final Instant epoch = Instant.EPOCH;
        final Instant current = Instant.now();
        final Duration duration = Duration.between(epoch, current);
        final long value = duration.getNano() + duration.getSeconds() * 1000000000L;

        System.out.println(value);
    }
}
