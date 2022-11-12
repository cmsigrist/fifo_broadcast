package cs451.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

public class AtomicMap<K, V> {
  private final HashMap<K, V> map;
  public final ReentrantLock lock;

  public AtomicMap() {
    this.map = new HashMap<>();
    this.lock = new ReentrantLock();
  }

  public ArrayList<V> snapshot() {
    ArrayList<V> copy;
    lock.lock();

    try {
      copy = new ArrayList<>(map.values());
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public HashMap<K, V> copy() {
    HashMap<K, V> copy;
    lock.lock();

    try {
      copy = new HashMap<>(map);
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public void put(K key, V value) {
    lock.lock();

    try {
      map.put(key, value);
    } finally {
      lock.unlock();
    }
  }

  public void nonAtomicPut(K key, V value) {
    map.put(key, value);
  }

  // Return V or null if V does not exist in the map
  public V get(K key) {
    V value;
    lock.lock();

    try {
      value = map.get(key);
    } finally {
      lock.unlock();
    }

    return value;
  }

  public V nonAtomicGet(K key) {
    return map.get(key);
  }

  public void remove(K key) {
    lock.lock();

    try {
      map.remove(key);
    } finally {
      lock.unlock();
    }
  }
}
