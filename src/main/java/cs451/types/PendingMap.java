package cs451.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class PendingMap {
  // HashMap<pid, HashMap<SeqNum, ArrayList<PendingAck>>
  private final HashMap<Byte, HashMap<Integer, ArrayList<PendingAck>>> map;
  private final ReentrantLock lock;

  public PendingMap() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public HashMap<Byte, HashMap<Integer, ArrayList<PendingAck>>> snapshot() {
    HashMap<Byte, HashMap<Integer, ArrayList<PendingAck>>> copy = new HashMap<>();

    lock.lock();
    try {
      // copy = new HashMap<>(map);
      for (var pid : map.entrySet()) {
        HashMap<Integer, ArrayList<PendingAck>> hCopy = new HashMap<>();

        for (var seqNum : pid.getValue().entrySet()) {
          ArrayList<PendingAck> pCopy = new ArrayList<>(seqNum.getValue());
          hCopy.put(seqNum.getKey(), pCopy);
        }

        copy.put(pid.getKey(), hCopy);
      }
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public void put(Byte pid, int seqNum, PendingAck pendingAck) {
    lock.lock();
    try {
      this.nonAtomicPut(pid, seqNum, pendingAck);
    } finally {
      lock.unlock();
    }
  }

  public void nonAtomicPut(byte pid, int seqNum, PendingAck pendingAck) {
    HashMap<Integer, ArrayList<PendingAck>> h = map.get(pid);

    if (h == null) {
      h = new HashMap<>();
    }

    ArrayList<PendingAck> pendingAcks = h.get(seqNum);

    if (pendingAcks == null) {
      pendingAcks = new ArrayList<>();
    }

    pendingAcks.add(pendingAck);
    h.put(seqNum, pendingAcks);
    map.put(pid, h);
  }

  public void removePendingAck(Byte pid, int seqNum, Predicate<PendingAck> pred) {
    lock.lock();
    try {
      HashMap<Integer, ArrayList<PendingAck>> h = map.get(pid);

      if (h != null) {
        ArrayList<PendingAck> pendingAcks = map.get(pid).get(seqNum);

        if (pendingAcks != null) {
          pendingAcks.removeIf(pred);

          if (pendingAcks.isEmpty()) {
            h.remove(seqNum);
          }
        }

        if (h.isEmpty()) {
          map.remove(pid);
        }
      }

    } finally {
      lock.unlock();
    }
  }

  public void remove(byte pid, int seqNum) {
    lock.lock();
    try {
      HashMap<Integer, ArrayList<PendingAck>> h = map.get(pid);

      if (h != null) {
        h.remove(seqNum);
      }
    } finally {
      lock.unlock();
    }
  }

  public void nonAtomicRemove(byte pid, int seqNum, PendingAck p) {
    HashMap<Integer, ArrayList<PendingAck>> h = map.get(pid);

    if (h != null) {
      ArrayList<PendingAck> pendingAcks = map.get(pid).get(seqNum);

      if (pendingAcks != null) {
        pendingAcks.remove(p);
      }
    }
  }

  public int size() {
    int size = 0;

    lock.lock();
    try {
      for (HashMap<Integer, ArrayList<PendingAck>> h : map.values()) {
        for (ArrayList<PendingAck> pendingAcks : h.values()) {
          size += pendingAcks.size();
        }
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

  public boolean nonAtomicContains(byte pid, int seqNum, PendingAck p) {
    boolean contains = false;

    if (map.containsKey(pid)) {
      HashMap<Integer, ArrayList<PendingAck>> h = map.get(pid);

      if (h != null) {
        ArrayList<PendingAck> pendingAcks = h.get(seqNum);

        if (pendingAcks != null) {
          return pendingAcks.contains(p);
        }
      }
    }

    return contains;
  }
}
