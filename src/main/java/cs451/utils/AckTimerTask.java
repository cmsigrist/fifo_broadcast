package cs451.utils;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.time.Instant;
import java.util.TimerTask;

public class AckTimerTask extends TimerTask {
    private final PropertyChangeSupport mPcs = new PropertyChangeSupport(this);

    @Override
    public void run() {
        mPcs.firePropertyChange("timer",
                0, Instant.now());
    }

    public void addPropertyChangeListener(PropertyChangeListener listener) {
        mPcs.addPropertyChangeListener(listener);
    }

    public void removePropertyChangeListener(PropertyChangeListener listener) {
        mPcs.removePropertyChangeListener(listener);
    }
}
