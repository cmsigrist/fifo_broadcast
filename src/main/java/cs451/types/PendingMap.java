package cs451.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class PendingMap {
  private final HashMap<Integer, ArrayList<PendingAck>> map;
  private final ReentrantLock lock;

  public PendingMap() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public HashMap<Integer, ArrayList<PendingAck>> snapshot() {
    HashMap<Integer, ArrayList<PendingAck>> copy = new HashMap<>();

    lock.lock();
    try {
      for (var seqNum : map.entrySet()) {
        ArrayList<PendingAck> pCopy = new ArrayList<>(seqNum.getValue());
        copy.put(seqNum.getKey(), pCopy);
      }
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public void put(int seqNum, PendingAck pendingAck) {
    lock.lock();
    try {
      this.nonAtomicPut(seqNum, pendingAck);
    } finally {
      lock.unlock();
    }
  }

  public void nonAtomicPut(int seqNum, PendingAck pendingAck) {
    ArrayList<PendingAck> pendingAcks = map.get(seqNum);

    if (pendingAcks == null) {
      pendingAcks = new ArrayList<>();
    }

    pendingAcks.add(pendingAck);
    map.put(seqNum, pendingAcks);
  }

  public void removePendingAck(int seqNum, Predicate<PendingAck> pred) {
    lock.lock();
    try {
      ArrayList<PendingAck> pendingAcks = map.get(seqNum);

      if (pendingAcks != null) {
        pendingAcks.removeIf(pred);

        if (pendingAcks.isEmpty()) {
          map.remove(seqNum);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  public void nonAtomicRemove(int seqNum, PendingAck p) {
    ArrayList<PendingAck> pendingAcks = map.get(seqNum);

    if (pendingAcks != null) {
      pendingAcks.remove(p);
    }
  }

  public int size() {
    int size = 0;

    lock.lock();
    try {
      for (ArrayList<PendingAck> pendingAcks : map.values()) {
        size += pendingAcks.size();
      }
    } finally {
      lock.unlock();
    }

    return size;
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  public boolean nonAtomicContains(int seqNum, PendingAck p) {
    boolean contains = false;

    if (map.containsKey(seqNum)) {
      ArrayList<PendingAck> pendingAcks = map.get(seqNum);
      if (pendingAcks != null) {
        return pendingAcks.contains(p);
      }
    }

    return contains;
  }
}
