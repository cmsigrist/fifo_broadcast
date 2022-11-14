package cs451.types;

import java.util.HashSet;
import java.util.concurrent.locks.ReentrantLock;

public class AtomicSet<T> {
  private final HashSet<T> set;
  public final ReentrantLock lock;

  public AtomicSet() {
    set = new HashSet<>();
    lock = new ReentrantLock();
  }

  public void add(T e) {
    lock.lock();

    try {
      set.add(e);
    } finally {
      lock.unlock();
    }
  }

  public void remove(T e) {
    lock.lock();

    try {
      set.remove(e);
    } finally {
      lock.unlock();
    }
  }

  public boolean contains(T e) {
    boolean has = false;
    lock.lock();

    try {
      has = set.contains(e);
    } finally {
      lock.unlock();
    }

    return has;
  }

  public HashSet<T> snapshot() {
    HashSet<T> hashSet;

    lock.lock();

    try {
      hashSet = new HashSet<>(set);
    } finally {
      lock.unlock();
    }

    return hashSet;
  }
}
