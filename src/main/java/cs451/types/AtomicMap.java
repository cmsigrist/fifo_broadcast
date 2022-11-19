package cs451.types;

import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

public class AtomicMap<K, V> {
  private final HashMap<K, HashSet<V>> map;
  private final ReentrantLock lock;

  public AtomicMap() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public HashMap<K, HashSet<V>> snapshot() {
    HashMap<K, HashSet<V>> copy;
    lock.lock();

    try {
      copy = new HashMap<>(map);
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public void put(K key, V[] values) {
    HashSet<V> newValues;

    lock.lock();

    try {
      newValues = map.get(key);

      if (newValues == null) {
        newValues = new HashSet<>();
      }

      for (V value : values) {
        newValues.add(value);
      }

      map.put(key, newValues);
    } finally {
      lock.unlock();
    }
  }

  public void put(K key, V value) {
    HashSet<V> newValues;

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

  public HashSet<V> get(K key) {
    HashSet<V> value;
    lock.lock();

    try {
      value = map.get(key);
    } finally {
      lock.unlock();
    }

    return value;
  }

  public void remove(K key) {
    lock.lock();

    try {
      map.remove(key);
    } finally {
      lock.unlock();
    }
  }

  public void removeElem(K key, Predicate<V> pred) {
    lock.lock();

    try {
      if (map.containsKey(key)) {
        map.get(key).removeIf(pred);
      }
    } finally {
      lock.unlock();
    }
  }

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  public HashSet<V> nonAtomicGet(K key) {
    return map.get(key);
  }

  public void nonAtomicRemove(K key) {
    map.remove(key);
  }

  public void nonAtomicPut(K key, V value) {
    HashSet<V> newValues;

    newValues = map.get(key);

    if (newValues == null) {
      newValues = new HashSet<>();
    }

    newValues.add(value);
    map.put(key, newValues);
  }

  public boolean nonAtomicHasKey(K key) {
    return map.containsKey(key);
  }
}
