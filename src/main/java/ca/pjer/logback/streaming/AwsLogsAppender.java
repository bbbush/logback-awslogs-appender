package ca.pjer.logback.streaming;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.OutputStreamAppender;
import ch.qos.logback.core.status.ErrorStatus;
import com.amazonaws.services.logs.AWSLogs;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.DataAlreadyAcceptedException;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.InvalidSequenceTokenException;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;
import com.amazonaws.services.logs.model.ResourceAlreadyExistsException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.toList;

public class AwsLogsAppender<E> extends OutputStreamAppender<E> {

    private static final int MAX_EVENT_SIZE = 262144;
    private String logRegion;
    private String logGroupName;
    private String logStreamName;
    private long lastTimestamp;
    private Thread thread;

    @SuppressWarnings({"unused", "WeakerAccess"})
    public String getLogGroupName() {
        return logGroupName;
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void setLogGroupName(String logGroupName) {
        this.logGroupName = logGroupName;
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public String getLogStreamName() {
        return logStreamName;
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void setLogStreamName(String logStreamName) {
        this.logStreamName = logStreamName;
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public String getLogRegion() {
        return logRegion;
    }

    @SuppressWarnings({"unused", "WeakerAccess"})
    public void setLogRegion(String logRegion) {
        this.logRegion = logRegion;
    }

    @Override
    protected void append(E eventObject) {
        if (eventObject instanceof ILoggingEvent) {
            ILoggingEvent event = (ILoggingEvent) eventObject;
            if (lastTimestamp < event.getTimeStamp()) {
                lastTimestamp = event.getTimeStamp();
                if (lastTimestamp == 0) {
                    lastTimestamp = System.currentTimeMillis();
                }
            }
        }
        super.append(eventObject);
    }

    @Override
    public void start() {
        lock.lock();
        try {
            if (thread == null) {
                LogTarget logTarget = new LogTarget();
                setOutputStream(logTarget.outputStream);
                thread = new Thread(() -> this.transferLogs(logTarget.inputStream));
                thread.setDaemon(true);
                thread.start();
            }
        } finally {
            lock.unlock();
        }
        super.start();
    }

    private void transferLogs(InputStream inputStream) {
        AwsLogsStub awsLogsStub = new AwsLogsStub();
        AtomicBoolean stopped = new AtomicBoolean();
        BlockingQueue<String> queue = new LinkedBlockingQueue<>();
        Thread thread = new Thread(() -> {
            List<String> list = new ArrayList<>();
            while (!stopped.get() || !queue.isEmpty()) {
                if (!queue.isEmpty()) {
                    try {
                        queue.drainTo(list);
                        awsLogsStub.putLogEvents(list);
                    } catch (Exception e) {
                        addStatus(new ErrorStatus("drain logs", this, e));
                    } finally {
                        list.clear();
                    }
                } else {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        addStatus(new ErrorStatus("sleep", this, e));
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }
                String s = line.trim();
                if (!s.isEmpty()) {
                    queue.add(s);
                }
            }
        } catch (IOException e) {
            addStatus(new ErrorStatus("read lines", this, e));
        }
        stopped.set(true);
        try {
            thread.join();
        } catch (InterruptedException e) {
            addStatus(new ErrorStatus("wait", this, e));
        }
        awsLogsStub.stop();
    }

    @Override
    public void stop() {
        super.stop();
        lock.lock();
        try {
            if (thread != null) {
                thread.join();
                thread = null;
            }
        } catch (InterruptedException e) {
            addStatus(new ErrorStatus("stop thread", this, e));
        } finally {
            lock.unlock();
        }
    }

    private class LogTarget {

        private final PipedInputStream inputStream = new PipedInputStream();
        private final PipedOutputStream outputStream = new PipedOutputStream();

        private LogTarget() {
            try {
                inputStream.connect(outputStream);
            } catch (IOException e) {
                addStatus(new ErrorStatus("create log target", this, e));
            }
        }
    }

    private class AwsLogsStub {

        private final AWSLogs awsLogs = AWSLogsClient.builder()
                .withRegion(logRegion)
                .build();
        private final AtomicBoolean logCreated = new AtomicBoolean();
        private String sequenceToken;

        private void createLogGroup() {
            if (!logCreated.get()) {
                try {
                    awsLogs.createLogGroup(new CreateLogGroupRequest()
                            .withLogGroupName(logGroupName));
                } catch (ResourceAlreadyExistsException ignored) {
                }
                try {
                    awsLogs.createLogStream(new CreateLogStreamRequest()
                            .withLogGroupName(logGroupName)
                            .withLogStreamName(logStreamName));
                } catch (ResourceAlreadyExistsException ignored) {
                }
                logCreated.set(true);
            }
        }

        private void stop() {
            try {
                awsLogs.shutdown();
            } catch (Exception e) {
                addStatus(new ErrorStatus("shutdown awsLogs", this, e));
            }
        }

        private void putLogEvents(Collection<String> list) {
            List<InputLogEvent> logEvents = list.stream()
                    .map(s -> s.substring(0, Math.min(s.length(), MAX_EVENT_SIZE)))
                    .map(s -> new InputLogEvent()
                            .withTimestamp(lastTimestamp)
                            .withMessage(s))
                    .collect(toList());
            if (!logEvents.isEmpty()) {
                logPreparedEvents(logEvents);
            }
        }

        private void logPreparedEvents(Collection<InputLogEvent> events) {
            createLogGroup();
            try {
                PutLogEventsRequest request = new PutLogEventsRequest()
                        .withLogGroupName(logGroupName)
                        .withLogStreamName(logStreamName)
                        .withSequenceToken(sequenceToken)
                        .withLogEvents(events);
                PutLogEventsResult result = awsLogs.putLogEvents(request);
                sequenceToken = result.getNextSequenceToken();
            } catch (DataAlreadyAcceptedException e) {
                sequenceToken = e.getExpectedSequenceToken();
            } catch (InvalidSequenceTokenException e) {
                sequenceToken = e.getExpectedSequenceToken();
                logPreparedEvents(events);
            }
        }

    }
}
