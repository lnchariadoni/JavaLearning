package org.example.CollectionsTests;

import jdk.jfr.Description;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class UnmodifiableTest {

  @Test
  @Description("When original list is modified, unmodifiable list reflects the changes")
  void mutationOfUnmodifiableList() {
    List<String> originalList = new ArrayList<>(List.of("cat", "ball", "apple"));
    List<String> unmodifiableCopyList = Collections.unmodifiableList(originalList);

    assertThrows(UnsupportedOperationException.class,
        () -> unmodifiableCopyList.add("new element"));

    originalList.add("zebra");

    var expected = List.of("cat", "ball", "apple", "zebra");
    assertEquals(expected, originalList);
  }
}
