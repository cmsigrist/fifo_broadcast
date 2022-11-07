package cs451.types;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class AtomicArrayList<T> {
  private final ArrayList<T> elements;
  private final ReentrantLock lock;

  public AtomicArrayList() {
    this.elements = new ArrayList<>();
    this.lock = new ReentrantLock();
  }

  public void add(T e) {
    lock.lock();

    try {
      elements.add(e);
    } finally {
      lock.unlock();
    }
  }

  public void remove(T e) {
    lock.lock();

    try {
      elements.remove(e);
    } finally {
      lock.unlock();
    }
  }

  public void addIfNotInArray(T e) {
    lock.lock();

    try {
      if (!elements.contains(e)) {
        elements.add(e);
      }
    } finally {
      lock.unlock();
    }
  }

  public ArrayList<T> snapshot() {
    ArrayList<T> copy = new ArrayList<>();
    lock.lock();

    try {
      for (T e : elements) {
        copy.add(e);
      }
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public ArrayList<T> nonAtomicSnapshot() {
    ArrayList<T> copy = new ArrayList<>();

    for (T e : elements) {
      copy.add(e);
    }

    return copy;
  }

}
