package cs451.utils;

import java.util.Timer;

public class AckTimer {
    public static void startTimer(AckTimerTask task) {
        new Timer().scheduleAtFixedRate(task, 0, 100);
    }
}
