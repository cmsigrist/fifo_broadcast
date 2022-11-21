package cs451.types;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class Logs {
  private final ArrayList<String> elements;
  private final ReentrantLock lock;

  public Logs() {
    this.elements = new ArrayList<>();
    this.lock = new ReentrantLock();
  }

  public void add(String s) {
    lock.lock();

    try {
      elements.add(s);
    } finally {
      lock.unlock();
    }
  }

  public void remove(String s) {
    lock.lock();

    try {
      elements.remove(s);
    } finally {
      lock.unlock();
    }
  }

  public void addIfNotInArray(String s) {
    lock.lock();

    try {
      if (!elements.contains(s)) {
        elements.add(s);
      }
    } finally {
      lock.unlock();
    }
  }

  public ArrayList<String> snapshot() {
    ArrayList<String> copy = new ArrayList<>();
    lock.lock();

    try {
      for (String s : elements) {
        copy.add(s);
      }
    } finally {
      lock.unlock();
    }

    return copy;
  }

  public ArrayList<String> nonAtomicSnapshot() {
    ArrayList<String> copy = new ArrayList<>();

    for (String s : elements) {
      copy.add(s);
    }

    return copy;
  }

}
