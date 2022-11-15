package cs451.types;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class AtomicMap<K, V, T> {
  private final HashMap<K, HashMap<V, T>> map;
  private final ReentrantLock lock;

  public AtomicMap() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public HashMap<K, HashMap<V, T>> copy() {
    HashMap<K, HashMap<V, T>> copy;
    lock.lock();

    try {
      copy = new HashMap<>(map);
    } finally {
      lock.unlock();
    }

    return copy;
  }

  // Only use when T == V
  @SuppressWarnings("unchecked")
  public void put(K key, V[] values) {
    HashMap<V, T> newValues;

    lock.lock();

    try {
      newValues = map.get(key);

      if (newValues == null) {
        newValues = new HashMap<>();
      }

      for (V value : values) {
        newValues.put(value, (T) value);
      }

      map.put(key, newValues);
    } finally {
      lock.unlock();
    }
  }

  public void put(K key, V valueKey, T value) {
    HashMap<V, T> newValues;

    lock.lock();

    try {
      newValues = map.get(key);

      if (newValues == null) {
        newValues = new HashMap<>();
      }

      newValues.put(valueKey, value);
      map.put(key, newValues);
    } finally {
      lock.unlock();
    }
  }

  // Return V or null if V does not exist in the map
  public HashMap<V, T> get(K key) {
    HashMap<V, T> value;
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

  public boolean hasKey(K key) {
    boolean hasKey = false;
    lock.lock();

    try {
      hasKey = map.containsKey(key);
    } finally {
      lock.unlock();
    }

    return hasKey;
  }

  // Non atomic function to be used in combination when
  // process has already acquired the lock

  public void acquireLock() {
    lock.lock();
  }

  public void releaseLock() {
    lock.unlock();
  }

  public HashMap<V, T> nonAtomicGet(K key) {
    return map.get(key);
  }

  public void nonAtomicRemove(K key) {
    map.remove(key);
  }

  public void nonAtomicPut(K key, V valueKey, T value) {
    HashMap<V, T> newValues;

    newValues = map.get(key);

    if (newValues == null) {
      newValues = new HashMap<>();
    }

    newValues.put(valueKey, value);
    map.put(key, newValues);
  }

  public boolean nonAtomicHasKey(K key) {
    return map.containsKey(key);
  }
}
