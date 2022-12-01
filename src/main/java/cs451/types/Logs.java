package cs451.types;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

public class Logs {
  private final String[] elements;
  private final ReentrantLock lock;

  public Logs(int size) {
    this.elements = new String[size];
    this.lock = new ReentrantLock();
  }

  public void add(int step, String s) {
    lock.lock();

    try {
      elements[step] = s;
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
