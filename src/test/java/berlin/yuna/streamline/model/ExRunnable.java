package berlin.yuna.streamline.model;

@FunctionalInterface
public interface ExRunnable {
    @SuppressWarnings({"java:S112", "RedundantThrows"})
    void run() throws Exception;
}
