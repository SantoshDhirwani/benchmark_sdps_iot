package de.adrianbartnik.source.socket;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@PublicEvolving
public abstract class AbstractSocketSourceFunction<R> extends RichParallelSourceFunction<R>
        implements CheckpointedFunction, StoppableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractSocketSourceFunction.class);

    private static final String DELIMITER = "\n";

    private static final String STATE_HOSTNAMES = "hostnames";
    private static final String STATE_PORTS = "ports";
    private static final String STATE_RECORDS = "records";

    private static final long serialVersionUID = 1L;

    /**
     * Default connection timeout when connecting to the server socket (infinite).
     */
    private static final int CONNECTION_TIMEOUT_TIME = 0;

    private final List<String> hostnames;
    private final List<Integer> ports;

    private boolean restored;
    private String hostname;
    private int port;
    long numberProcessedMessages;

    private ListState<String> listStateHostnames;
    private ListState<Integer> listStatePorts;
    private ListState<Long> listStateNumberOfProcessedRecords;

    private transient Socket currentSocket;

    private volatile boolean isRunning = true;

    public AbstractSocketSourceFunction(List<String> hostnames, List<Integer> ports) {
        this.hostnames = checkNotNull(hostnames, "Hostnames must not be null");
        this.ports = checkNotNull(ports, "Ports must not be null");

        for (Integer port : ports) {
            checkArgument(port > 0 && port < 65536, "ports is out of range");
        }
    }

    @Override
    public void run(SourceContext<R> ctx) throws Exception {
        final StringBuilder buffer = new StringBuilder();

        checkArgument(hostnames.size() == getRuntimeContext().getNumberOfParallelSubtasks(),
                "Number of hostnames does not match degree of parallelism");

        try (Socket socket = new Socket()) {
            currentSocket = socket;

            hostname = chooseHostname();
            port = choosePort();
            numberProcessedMessages = restoreProcessedMessages();

            LOG.info("Connecting to server socket {}:{} with current number of messages {}",
                    hostname, port, numberProcessedMessages);

            socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintStream output = new PrintStream(socket.getOutputStream(), true);

            // Necessary, so port stays open after disconnect
            output.print(getStartCommand());

            char[] cbuf = new char[8192];
            int bytesRead;
            while (isRunning) {

                try {
                    if ((bytesRead = reader.read(cbuf)) == -1) {
                        LOG.error("SocketSource has reached end of file");
                        break;
                    }
                } catch (IOException exception) {
                    LOG.error("Error while reading from BufferedReader", exception);
                    break;
                }

                buffer.append(cbuf, 0, bytesRead);
                int delimPos;
                while (buffer.length() >= DELIMITER.length() && (delimPos = buffer.indexOf(DELIMITER)) != -1) {
                    String record = buffer.substring(0, delimPos);
                    // truncate trailing carriage return
                    if (record.endsWith("\r")) {
                        record = record.substring(0, record.length() - 1);
                    }

                    synchronized (ctx.getCheckpointLock()) {

                        if (!isRunning) {
                            return;
                        }

                        R tuple = stringToRecord(record);
                        ctx.collect(tuple); // Maybe collect already with timestamp?
                        numberProcessedMessages++;
                    }

                    buffer.delete(0, delimPos + DELIMITER.length());
                }
            }
        }
    }

    protected abstract R stringToRecord(String record);

    protected abstract String getStartCommand();

    private String chooseHostname() throws Exception {
        String localHostname = hostnames.get(getRuntimeContext().getIndexOfThisSubtask()), remoteHostname = "";

        if (hostnames.size() == 1 && hostnames.get(0).equals("l")) {
            return InetAddress.getLocalHost().getHostName();
        }

        for (String hostname : listStateHostnames.get()) {
            remoteHostname = hostname;
        }

        if (restored && !localHostname.equals(remoteHostname)) {
            throw new IllegalStateException("Restored hostname differs from originally assigned");
        }

        return localHostname;
    }

    private int choosePort() throws Exception {
        int localPort = ports.get(getRuntimeContext().getIndexOfThisSubtask()), restoredPort = -1;

        for (Integer port : listStatePorts.get()) {
            restoredPort = port;
        }

        if (restored && localPort != restoredPort) {
            throw new IllegalStateException("Restored port differs from originally assigned");
        }

        return localPort;
    }

    private long restoreProcessedMessages() throws Exception {
        if (!restored) {
            return 0;
        } else {

            long messages = -1;

            for (Long numberOfMessages : listStateNumberOfProcessedRecords.get()) {
                if (messages == -1) {
                    messages = numberOfMessages;
                } else {
                    throw new IllegalStateException("Multiple number of processed records");
                }
            }

            if (messages == -1) {
                throw new IllegalStateException("Not old number of messages could be restored");
            }

            return messages;
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

        // we need to close the socket as well, because the Thread.interrupt() function will
        // not wake the thread in the socketStream.read() method when blocked.
        Socket theSocket = this.currentSocket;
        if (theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        LOG.debug("Taking checkpoint of source {} of host {} on port {} with messages {} and is running? {}",
                getRuntimeContext().getIndexOfThisSubtask(), hostname, port, numberProcessedMessages, isRunning);

        listStateHostnames.clear();
        listStatePorts.clear();
        listStateNumberOfProcessedRecords.clear();

        listStateHostnames.add(hostname);
        listStatePorts.add(port);
        listStateNumberOfProcessedRecords.add(numberProcessedMessages);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        OperatorStateStore stateStore = context.getOperatorStateStore();

        listStateHostnames = stateStore.getListState(new ListStateDescriptor<>(STATE_HOSTNAMES, String.class));
        listStatePorts = stateStore.getListState(new ListStateDescriptor<>(STATE_PORTS, Integer.class));
        listStateNumberOfProcessedRecords = stateStore.getListState(new ListStateDescriptor<>(STATE_RECORDS, Long.class));

        if (context.isRestored()) {

            Preconditions.checkArgument(Iterables.size(listStateHostnames.get()) == 1,
                    "More than one hostname received");
            Preconditions.checkArgument(Iterables.size(listStatePorts.get()) == 1,
                    "More than one port received");
            Preconditions.checkArgument(Iterables.size(listStateNumberOfProcessedRecords.get()) == 1,
                    "More than one recorded message state received");

            restored = true;
        } else {
            LOG.info("No restore state for ParallelSocketSource");
        }
    }

    @Override
    public void stop() {
        isRunning = false;

        // we need to close the socket as well, because the Thread.interrupt() function will
        // not wake the thread in the socketStream.read() method when blocked.
        Socket theSocket = this.currentSocket;
        if (theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }
    }
}