package de.adrian.thesis.generator.benchmark.netty.creators;

import com.google.common.util.concurrent.RateLimiter;
import de.adrian.thesis.generator.benchmark.javaio.CreatorThread;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Abstract creator for the NexmarkBenchmark
 */
public abstract class AbstractNettyCreatorThread extends Thread {
    private static final Logger LOG = LogManager.getLogger(AbstractNettyCreatorThread.class);
    private final BlockingQueue<String> queue;
    private final AbstractNettyCreatorThreadProperties properties;
    private volatile boolean interrupted = false;
    long counter = 0;

    AbstractNettyCreatorThread(String threadName, AbstractNettyCreatorThreadProperties properties) {
        super(threadName);
        this.queue = new LinkedBlockingQueue<>();
        this.properties = properties;
    }

    @Override
    public void run() {
        System.out.println("Starting NettyCreator, parameter msgs/s:" + properties.messagesPerSecond);
        final int defaultSpeed = properties.messagesPerSecond;  //default throughput msg/s taken from parameter
        // Generate initial numbers
        for (; counter < properties.initialRecords && !interrupted; counter++) {
            String record = generateRecord(counter);
            queue.add(record);
            if (properties.logMessages && counter % properties.logMessagesModulo == 0) {
                LOG.info("Initially inserted '{}' into queue", record);
            }
        }
        System.out.println("Initial records generated, no: " + queue.size());
//        int randThroughput = ThreadLocalRandom.current().nextInt(5000, 10000);
//        RateLimiter throughputThrottler = RateLimiter.create(randThroughput, 5, TimeUnit.SECONDS);
        while (counter < properties.maxNumbers && !interrupted) {
            // varys throughput 40%+/- base load
            this.properties.messagesPerSecond = ThreadLocalRandom.current().nextInt((int)(defaultSpeed * ((double)6/10)), (int)(defaultSpeed * ((double)14/10)));
//            System.out.println("New speed is " + properties.messagesPerSecond);
            long emitStartTime = System.currentTimeMillis();
            int i = 0;
            for (; i < properties.messagesPerSecond && counter + i < properties.maxNumbers; i++) {
//                throughputThrottler.acquire(1);
                String record = generateRecord(counter + i);
                queue.add(record);
            }
            counter += i;
            System.out.println(counter + " records generated with new speed: " + properties.messagesPerSecond + " msg/s");

            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                try {
                    Thread.sleep(1000 - emitTime);
                } catch (InterruptedException e) {
                    LOG.error("NettyCreatorThread was interrupted: {}", e.getLocalizedMessage());
                    break;
                }
            }
        }
    }

    abstract String generateRecord(long currentNumber);

    public abstract String getShortDescription();

    public void stopProducing() {
        interrupt();
        interrupted = true;
    }

    public BlockingQueue<String> getQueue() {
        return queue;
    }

    public static class AbstractNettyCreatorThreadProperties extends CreatorThread.CreateThreadProperties {

        int initialRecords;

        public AbstractNettyCreatorThreadProperties(CreatorThread.CreateThreadProperties properties) {
            this.messagesPerSecond = properties.messagesPerSecond;
            this.logMessages = properties.logMessages;
            this.logMessagesModulo = properties.logMessagesModulo;
            this.maxNumbers = properties.maxNumbers;
        }

        public AbstractNettyCreatorThreadProperties setInitialRecords(int initialRecords) {
            this.initialRecords = initialRecords;
            return this;
        }
    }
}
