package cs451.types;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class DeliveredMap {
  private final HashMap<Byte, Integer> map;
  public final ReentrantLock lock;

  public DeliveredMap() {
    map = new HashMap<>();
    lock = new ReentrantLock();
  }

  public void put(byte pid, int seqNum) {
    lock.lock();
    try {
      int s = seqNum;

      if (map.containsKey(pid)) {
        if (map.get(pid) > seqNum) {
          s = map.get(pid);
        }
      }

      map.put(pid, s);
    } finally {
      lock.unlock();
    }
  }

  public int get(byte pid) {
    int s = 0;

    lock.lock();
    try {
      if (map.containsKey(pid)) {
        s = map.get(pid);
      }
    } finally {
      lock.unlock();
    }

    return s;
  }

  public boolean contains(byte pid, int seqNum) {
    boolean has = false;

    lock.lock();
    try {
      if (map.containsKey(pid)) {
        has = seqNum <= map.get(pid);
      }
    } finally {
      lock.unlock();
    }

    return has;
  }
}
