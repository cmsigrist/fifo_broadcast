package cs451.types;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class ForwardedMap {
  private final HashMap<Byte, Forwarded> map;
  private final ReentrantLock lock;

  public ForwardedMap() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public HashMap<Byte, Forwarded> snapshot() {
    HashMap<Byte, Forwarded> copy;

    lock.lock();
    try {
      copy = new HashMap<>(map);
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public void put(byte pid, int seqNum) {
    lock.lock();
    try {
      Forwarded forwarded = map.get(pid);

      if (forwarded == null) {
        forwarded = new Forwarded(seqNum);
      } else {
        forwarded.update(seqNum);
      }

      map.put(pid, forwarded);
    } finally {
      lock.unlock();
    }
  }

  public Forwarded get(byte pid) {
    Forwarded value;

    lock.lock();
    try {
      value = map.get(pid);
    } finally {
      lock.unlock();
    }

    return value;
  }

  public boolean contains(byte pid, int seqNum) {
    boolean contains = false;

    lock.lock();
    try {
      if (map.containsKey(pid)) {
        contains = map.get(pid).contains(seqNum);
      }
    } finally {
      lock.unlock();
    }

    return contains;
  }

  public void cleanUp(byte pid, int range) {
    lock.lock();
    try {
      if (map.containsKey(pid)) {
        map.get(pid).cleanUp(range);
      }
    } finally {
      lock.unlock();
    }
  }
}
