package cs451.types;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

public class AckMap {
  private final HashMap<Integer, HashSet<Integer>> map;
  private final ReentrantLock lock;

  public AckMap() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public int size(int key) {
    int size = 0;

    lock.lock();
    try {
      if (map.containsKey(key)) {
        size += map.get(key).size();
      }
    } finally {
      lock.unlock();
    }

    return size;
  }

  public void put(int key, int[] values) {
    HashSet<Integer> newValues;

    lock.lock();
    try {
      newValues = map.get(key);

      if (newValues == null) {
        newValues = new HashSet<>();
      }

      for (int value : values) {
        newValues.add(value);
      }

      map.put(key, newValues);
    } finally {
      lock.unlock();
    }
  }

  public void put(int key, int value) {
    HashSet<Integer> newValues;

    lock.lock();
    try {
      newValues = map.get(key);

      if (newValues == null) {
        newValues = new HashSet<>();
      }

      newValues.add(value);

      map.put(key, newValues);
    } finally {
      lock.unlock();
    }
  }

  public HashSet<Integer> get(int key) {
    HashSet<Integer> value;

    lock.lock();
    try {
      value = map.get(key);
    } finally {
      lock.unlock();
    }

    return value;
  }

  public boolean remove(int key) {
    boolean removed = false;

    lock.lock();
    try {
      var r = map.remove(key);

      removed = r != null;
    } finally {
      lock.unlock();
    }

    return removed;
  }
}
