package org.example.GathererTests;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Gatherer;
import java.util.stream.Gatherers;
import jdk.jfr.Description;
import org.junit.jupiter.api.Test;

class GatherersPrebuiltTest {
  static final List<Integer> inputList = List.of(1, 2, 3, 4, 5);

  @Test
  @Description("Test for Gatherers.fold which is similar to reduce but can emit intermediate results")
  void foldTest() {
    var result = inputList
        .stream()
        .gather(Gatherers.fold(() -> 0, Integer::sum))
        .toList();

    List<Integer> expected = List.of(15);
    assertEquals(expected, result);
  }

  @Test
  @Description("Test for Gatherers.windowFixed which creates fixed size windows of elements")
  void windowFixedTest() {
    var result = inputList
        .stream()
        .gather(Gatherers.windowFixed(3))
        .toList();

    List<List<Integer>> expected = List.of(List.of(1, 2, 3), List.of(4, 5));

    assertEquals(expected, result);
  }

  @Test
  @Description("Test for Gatherers.windowSliding which creates sliding windows of elements")
  void windowSlidingTest() {
    var result = inputList
        .stream()
        .gather(Gatherers.windowSliding(3))
        .toList();

    List<List<Integer>> expected = List.of(List.of(1, 2, 3), List.of(2, 3, 4), List.of(3, 4, 5));

    assertEquals(expected, result);
  }

  @Test
  @Description("Computing of running total using Gatherer and Sequential.")
  void runningTotalUsingSequentialTest() {
    Gatherer<Integer, ArrayList<Integer>, Object> sequentitalGatharerRunningTotal =
        Gatherer.ofSequential(
            () -> {
              System.out.println("In Initializer: " + Thread.currentThread().getName());
              return new ArrayList<>();
            },
            ((state, element, downstream) -> {
              var lastValue = state.isEmpty() ? 0 : state.getLast();
              var oldState = new ArrayList<>(state);
              state.add(lastValue + element);

              System.out.println("In Integrator: "
                  + Thread.currentThread().getName()
                  + " old state:" + oldState + " , state:" + state + ", element:" + element
                  + ", downstream:" + downstream);

              return true;
            }),
            (state, downstream) -> {
              System.out.println("In Finisher: " + Thread.currentThread().getName()
                  + " , state:" + state + ", downstream:" + downstream);
              state.forEach(downstream::push);
            }
        );
    var result = inputList
        .stream()
        .parallel()
        .gather(sequentitalGatharerRunningTotal)
        .toList();

    List<Integer> expected = List.of(1, 3, 6, 10, 15);
    assertEquals(expected, result);
  }

  @Test
  @Description("This uses Gatherer.of so both sequential and parallel will work")
  void runningTotalTest() {
    Gatherer<Integer, List<Integer>, Integer> runningTotalGatherer = Gatherer.of(
        () -> {
          System.out.println("In Initializer: " + Thread.currentThread().getName());
          return new ArrayList<>();
        },                           // initializer
        (state, item, downstream) -> { // integrator
          System.out.println("In Integrator: "
              + Thread.currentThread().getName()
              + " , state:" + state + ", item:" + item + ", downstream:" + downstream);

          int last = state.isEmpty() ? 0 : state.getLast();
          var newElement = last + item;
          state.add(newElement);
          return true;
        },
        (left, right) -> { // combiner
          System.out.println("In Combiner: "
              + Thread.currentThread().getName()
              + " , left:" + left + ", right:" + right);

          if (left.isEmpty()) {
            return right;
          }

          if (right.isEmpty()) {
            return left;
          }

          int offset = left.getLast();
          left.addAll(right.stream().map(i -> i + offset).toList());
          return left;
        },
        (state, downstream) -> {                 // finisher
          System.out.println("In Finisher: "
              + Thread.currentThread().getName()
              + " , state:" + state + ", downstream:" + downstream);
          state.forEach(downstream::push);
        }
    );

    var result = inputList
        .stream()
        .parallel()
        .gather(runningTotalGatherer)
        .toList();

    List<Integer> expected = List.of(1, 3, 6, 10, 15);
    assertEquals(expected, result);
  }

  @Test
  @Description("code to test parallel short circuting of the gatherer")
  void parallelGathererShortCircuitTest() {
    List<Integer> result = inputList
        .parallelStream()
        .gather(Gatherer.<Integer, boolean[], Integer>of(
                () -> new boolean[] {true},
                (state, element, downStram) -> {
                  if (!state[0]) {
                    return false;
                  }

                  if (element >= 5) {
                    state[0] = false;
                    return false;
                  }

                  downStram.push(element);
                  return true;
                },
                (left, right) -> left,
                (state, downStream) -> {
                }
            )
        )
        .toList();

    List<Integer> expected = List.of(1, 2, 3, 4);
    assertEquals(expected, result);
  }
}
