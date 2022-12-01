package cs451.types;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class AckCount {
  private final HashMap<Integer, Integer> map;
  private final ReentrantLock lock;

  public AckCount() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public void set(int step, int value) {
    lock.lock();
    try {
      map.put(step, value);
    } finally {
      lock.unlock();
    }
  }

  public int incrementAndGet(int step) {
    int newCount = 0;

    lock.lock();
    try {
      if (map.containsKey(step)) {
        newCount = map.get(step);
      }
      newCount += 1;
      map.put(step, newCount);
    } finally {
      lock.unlock();
    }

    return newCount;
  }

  public int get(int step) {
    int newCount = 0;

    lock.lock();
    try {
      if (map.containsKey(step)) {
        newCount = map.get(step);
      }
    } finally {
      lock.unlock();
    }

    return newCount;
  }

  public void remove(int step) {
    lock.lock();
    try {
      map.remove(step);
    } finally {
      lock.unlock();
    }
  }
}
