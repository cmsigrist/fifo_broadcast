package cs451.types;

import java.util.concurrent.locks.ReentrantLock;

public class Logs {
  private final String[] elements;
  private final ReentrantLock lock;
  private int size;

  public Logs(int size) {
    this.elements = new String[size];
    this.lock = new ReentrantLock();
    this.size = 0;
  }

  public void add(int step, String s) {
    lock.lock();

    try {
      elements[step] = s;
      size += 1;
    } finally {
      lock.unlock();
    }
  }

  public String[] snapshot() {
    System.out.println("size: " + size);
    String[] copy = new String[size];

    for (int i = 0; i < size; i++) {
      copy[i] = elements[i];
    }

    return copy;
  }

}
