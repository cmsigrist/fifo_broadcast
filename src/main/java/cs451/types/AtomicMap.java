package cs451.types;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class AtomicMap {
  private final HashMap<Byte, Forwarded> map;
  private final ReentrantLock lock;

  public AtomicMap() {
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

  public void put(Byte key, int seqNum) {
    lock.lock();
    try {
      Forwarded forwarded = map.get(key);

      if (forwarded == null) {
        forwarded = new Forwarded(seqNum);
      } else {
        forwarded.update(seqNum);
      }

      map.put(key, forwarded);
    } finally {
      lock.unlock();
    }
  }

  public Forwarded get(Byte key) {
    Forwarded value;

    lock.lock();
    try {
      value = map.get(key);
    } finally {
      lock.unlock();
    }

    return value;
  }

  public boolean contains(Byte key, int seqNum) {
    boolean contains = false;

    lock.lock();
    try {
      if (map.containsKey(key)) {
        contains = map.get(key).contains(seqNum);
      }
    } finally {
      lock.unlock();
    }

    return contains;
  }
}
