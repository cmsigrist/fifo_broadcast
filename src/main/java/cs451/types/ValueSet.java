package cs451.types;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

public class ValueSet {
  private final HashMap<Integer, HashSet<String>> map;
  private final ReentrantLock lock;

  public ValueSet() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public void add(int step, String newValue) {
    lock.lock();
    try {
      HashSet<String> values = map.get(step);

      if (values == null) {
        values = new HashSet<>();
      }

      values.add(newValue);
      map.put(step, values);
    } finally {
      lock.unlock();
    }
  }

  public void add(int step, String[] newValues) {
    lock.lock();
    try {
      HashSet<String> values = map.get(step);

      if (values == null) {
        values = new HashSet<>();
      }

      for (String value : newValues) {
        values.add(value);
      }

      map.put(step, values);
    } finally {
      lock.unlock();
    }
  }

  public String[] getToList(int step) {
    String[] set = new String[0];

    lock.lock();
    try {
      if (map.containsKey(step)) {
        set = map.get(step).toArray(set);
      }
    } finally {
      lock.unlock();
    }

    return set;
  }

  public HashSet<String> get(int step) {
    HashSet<String> set;

    lock.lock();
    try {
      if (map.containsKey(step)) {
        set = new HashSet<>(map.get(step));
      } else {
        set = new HashSet<>();
      }
    } finally {
      lock.unlock();
    }

    return set;
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