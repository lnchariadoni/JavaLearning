package org.example.GathererTests;

import jdk.jfr.Description;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.*;

class GatherersPrebuiltTest {
    static List<Integer> inputList = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    @Test
    void fold() {
        var result = inputList
                .stream()
                .gather(Gatherers.fold(() -> 0, Integer::sum))
                .toList(); // [1, 3, 6, 10, 15] - running sum

        System.out.println("result:" + result);
    }

    @Test
    void windowFixed() {
        var result = inputList
                .stream().gather(Gatherers.windowFixed(3))
                .toList();

        System.out.println("result:" + result);
    }

    @Test
    void windowSliding() {
        var result = inputList
                .stream().gather(Gatherers.windowSliding(3))
                .toList();

        System.out.println("result:" + result);
    }

    @Test
    @Description("Computing of running total using Gatherer and Sequential.")
    void runningTotalUsingSequential() {
        Gatherer<Integer, ArrayList<Integer>, Object> sequentitalGatharerRunningTotal = Gatherer.ofSequential(
                () -> {
                    System.out.println("In Initializer: " + Thread.currentThread().getName());
                    return new ArrayList<Integer>();
                },
                ((state, element, downstream) -> {
                    var lastValue = state.isEmpty() ? 0 : state.getLast();
                    var oldState = new ArrayList<>(state);
                    state.add(lastValue + element);

                    System.out.println("In Integrator: " + Thread.currentThread().getName()
                            + " old state:" + oldState + " , state:" + state + ", element:" + element + ", downstream:" + downstream);

                    return true;
                }),
                (state, downstream) -> {
                    System.out.println("In Finisher: " + Thread.currentThread().getName()
                            + " , state:" + state + ", downstream:" + downstream);
                    state.forEach(downstream::push);
                }
        );
        var result = inputList.stream()
                .parallel()
                .gather(sequentitalGatharerRunningTotal)
                .toList();

        System.out.println("result: " + result);
    }

    @Test
    @Description("This uses Gatherer.of so both sequential and parallel will work")
    void runningTotal() {
        Gatherer<Integer, List<Integer>, Integer> runningTotalGatherer = Gatherer.of(
                () -> {
                    System.out.println("In Initializer: " + Thread.currentThread().getName());
                    return new ArrayList<>();
                },                           // initializer
                (state, item, downstream) -> { // integrator
                    System.out.println("In Integrator: " + Thread.currentThread().getName()
                            + " , state:" + state + ", item:" + item + ", downstream:" + downstream);

                    int last = state.isEmpty() ? 0 : state.getLast();
                    var newElement = last + item;
                    state.add(newElement);
                    return true;
                },
                (left, right) -> {                        // combiner
                    System.out.println("In Combiner: " + Thread.currentThread().getName()
                            + " , left:" + left + ", right:" + right);

                    if (left.isEmpty()) return right;
                    if (right.isEmpty()) return left;
                    int offset = left.getLast();
                    left.addAll(right.stream().map(i -> i + offset).toList());
                    return left;
                },
                (state, downstream) -> {                 // finisher
                    System.out.println("In Finisher: " + Thread.currentThread().getName()
                            + " , state:" + state + ", downstream:" + downstream);
                    state.forEach(downstream::push);
                }
        );

        var result = inputList
                .stream()
                .parallel()
                .gather(runningTotalGatherer)
                .toList();

        System.out.println("result:" + result);
    }

    @Test
    @Description("code to test parallel short circuting of the gatherer")
    void parallelGathererShortCircuit() {
        List.of(1, 2, 3, 6, 7, 8, 10)
                .parallelStream()
                .gather(Gatherer.<Integer, boolean[], Integer>of(
                                () -> new boolean[]{true},
                                (state, element, downStram) -> {
//                            try { Thread.sleep(100 * element); } catch(Exception e) {}

//                            System.out.println("In Integrator: " + Thread.currentThread().getName()
//                                    + " , state:" + state[0] + ", element:" + element );
                                    if (state[0] == false) return false;

                                    if (element >= 5) {
                                        state[0] = false;
                                        return state[0];
                                    }

                                    downStram.push(element);
                                    return true;
                                },
                                (left, right) -> left,
                                (state, downStream) -> {
                                }
                        )
                )
//                .forEach(System.out::println);
                .forEachOrdered(System.out::println);

//        System.out.println("result:" + result);
    }

    @Test
    void ordervsUnOrderedList() {
        List<Integer> list = IntStream.rangeClosed(1, 10).boxed().toList();

        System.out.println("forEach:");
        list.parallelStream().forEach(x -> System.out.print(x + " "));
// Possible output: 6 7 8 9 10 1 2 3 4 5

        System.out.println("forEachOrdered:");
        list.parallelStream().forEachOrdered(x -> System.out.print(x + " "));
// Guaranteed output: 1 2 3 4 5 6 7 8 9 10

    }

    @Test
    void ordervsUnOrderedSet() {
        Set<Integer> list = IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toSet());
        System.out.println("input:" + list);

        System.out.println("forEach:");
        list.parallelStream().forEach(x -> System.out.print(x + " "));
// Possible output: 6 7 8 9 10 1 2 3 4 5

        System.out.println("forEachOrdered:");
        list.parallelStream().forEachOrdered(x -> System.out.print(x + " "));
// Guaranteed output: 1 2 3 4 5 6 7 8 9 10

    }
}
