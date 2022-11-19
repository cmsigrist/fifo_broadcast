package cs451.types;

import java.util.HashSet;

public class Forwarded {
  private int range;
  // put in seqNum if not continuous range
  private HashSet<Integer> seqNums;

  public Forwarded(int seqNum) {
    this.range = 1;
    this.seqNums = new HashSet<>();

    if (seqNum != 1) {
      this.range = 0;
      this.seqNums.add(seqNum);
    }
  }

  public void setRange(int range) {
    this.range = range;
  }

  public void addSeqNum(int seqNum) {
    seqNums.add(seqNum);
  }

  public int getRange() {
    return range;
  }

  public HashSet<Integer> getSeqNums() {
    return seqNums;
  }

  public void update(int seqNum) {
    if (range + 1 == seqNum) {
      range += 1;
    } else {
      seqNums.add(seqNum);
    }

    cleanUp();
  }

  private void cleanUp() {
    boolean continuous = true;

    while (continuous) {
      if (seqNums.contains(range + 1)) {
        seqNums.remove(range + 1);
        range += 1;
      } else {
        continuous = false;
      }
    }
  }

  public boolean contains(int seqNum) {
    return seqNum <= range || seqNums.contains(seqNum);
  }
}
