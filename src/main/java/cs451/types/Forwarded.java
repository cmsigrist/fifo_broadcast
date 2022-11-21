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

  // TODO remove, for println only
  public Forwarded(Forwarded f) {
    this.range = f.getRange();
    this.seqNums = new HashSet<>(f.getSeqNums());
  }

  public void setRange(int range) {
    this.range = range;
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
    } else if (seqNum > range + 1) {
      seqNums.add(seqNum);
    }

    cleanUp();
  }

  // Add all seqNums that is continuous to range and remove from seqNums
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

  // Remove all of delivered from seqNums, add all of delivered to range
  public void cleanUp(int delivered) {
    int i = range;

    while (i <= delivered) {
      seqNums.remove(i);
      i++;
    }

    this.range = range > delivered ? range : delivered;
  }

  public boolean contains(int seqNum) {
    return seqNum <= range || seqNums.contains(seqNum);
  }

  @Override
  public String toString() {
    return "{range: " + range + " seqNums: " + seqNums + "}";
  }
}
