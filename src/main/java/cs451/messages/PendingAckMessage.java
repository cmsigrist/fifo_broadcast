package cs451.messages;

import cs451.utils.AckTimerTask;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.time.Duration;
import java.time.Instant;

public class PendingAckMessage implements PropertyChangeListener {
    Message message;
    private Instant start;
    // The address of the node from which we wait an ack
    private final String destIP;
    private final int destPort;
    private final AckTimerTask ackTimerTask;
    public static final int ACK_TIMEOUT = 500; // in milli

    private final PropertyChangeSupport mPcs = new PropertyChangeSupport(this);

    public PendingAckMessage(Message message, Instant start, AckTimerTask ackTimerTask) {
        this.message = message;
        this.start = start;
        this.destIP = message.getDestIP();
        this.destPort = message.getDestPort();
        this.ackTimerTask = ackTimerTask;

        ackTimerTask.addPropertyChangeListener(this);
    }

    public Message getMessage() {
        return message;
    }

    public void setAcked() {
        mPcs.firePropertyChange("acked",
                message.getSeqNum() + destIP + destPort, true);

        ackTimerTask.removePropertyChangeListener(this);
    }

    public String getKey() {
        return message.getSeqNum() + destIP + destPort;
    }

    public static String makeKey(int seqNum, String destIP, int destPort) {
        return seqNum + destIP + destPort;
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        mPcs.addPropertyChangeListener(listener);
    }

    public void removePropertyChangeListener(PropertyChangeListener listener) {
        mPcs.removePropertyChangeListener(listener);
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt) {
        String propertyName = evt.getPropertyName();

        if ("timer".equals(propertyName)) {
            System.out.println("PropertyChange in pending ack thread : " + Thread.currentThread().getId() + " timeout: ");
            Instant now = (Instant)evt.getNewValue();

            if (Duration.between(start, now).toMillis() >= ACK_TIMEOUT) {
                mPcs.firePropertyChange("timeout",
                        message.getSeqNum() + destIP + destPort, true);

                this.start = Instant.now();
            }
        }
    }
}
