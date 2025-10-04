package org.example.GathererTests;

import jdk.jfr.Description;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Gatherer;
import java.util.stream.Gatherers;
import java.util.stream.IntStream;

import static org.example.util.Utils.constructMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GatherersParallemStreamSequenceGathererTest {
    static List<Integer> inputList = List.of(1, 2, 3, 4, 5);

    static Predicate<Integer> greaterThanZero = x -> {
        System.out.println(constructMessage("in 'x>0' filter for x=" + x));
        return x > 0;
    };

    static Function<Integer, Integer> multiplyByTwo = x -> {
        System.out.println(constructMessage("in 'x*2' map for x=" + x));
        return x * 2;
    };

    static Function<Integer, Integer> multiplyByThree = x -> {
        System.out.println(constructMessage("in 'x*3' map for x=" + x));
        return x * 3;
    };

    static Predicate<Integer> lessThanTen = x -> {
        System.out.println(constructMessage("in 'x<10' filter for x=" + x));
        return x < 10;
    };

    static Predicate<Integer> lessThanHundred = x -> {
        System.out.println(constructMessage("in 'x<100' filter for x=" + x));
        return x < 100;
    };


    /*
        when only integrator exists, though the stream is parallel - the gatherer forces the upstream to be sequence.
        When the gatherer completes processing all operations, then only all elements are passed to the downstream in parallel - as the stream is parallel.
        In short, we can say till the gatherer the stream processing is sequential.
        Till gatherer, all elements starting from beginning of the parallel stream run in a single thread.
     */
    static Gatherer<Integer, Void, Integer> onlyIntegratorSequenceGatherer = Gatherer.ofSequential(
            (Void _, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                System.out.println(constructMessage("in gatherer with only integrator for x=" + x));
                return result.push(x);
            });

    /*
        This is similar to sequence gatherer with only integrator. The only difference is that the finisher is called only once when integrator processes all
        elements in sequence.
        Only after that all processed elements by gatherer are passed to downstream in parallel.
     */
    static Gatherer<Integer, Void, Integer> integratorWithFinisherSequenceGatherer = Gatherer.ofSequential(
            (Void _, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                System.out.println(constructMessage("in gatherer with only integrator for x=" + x));
                return result.push(x);
            },
            (Void _, Gatherer.Downstream<? super Integer> result) -> {
                result.push(11);
                System.out.println(constructMessage("in gatherer finisher"));
            }
    );

    /*
      This is similar to above. The only difference is that the initializer is called only once. The single thread which is executing the gatherer for all the
      upstream elements.
    */
    static Gatherer<Integer, int[], Integer> initializerWithIntegratorSequenceGatherer = Gatherer.ofSequential(
            () -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator in initializer"));
                return new int[]{0};
            },
            (int[] state, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                state[0] += x;
                System.out.println(constructMessage("in gatherer with initializer & integrator in integrator for x=" + x));
                return result.push(state[0]);
            });

    /*
      This is same as above with initializer. When gatherer is done with processing all elements, the finisher is called only once.
      Only then, all elements are in parallel processed in the downstream.
    */
    static Gatherer<Integer, int[], Integer> initializerWithIntegratorAndFinisherSequenceGatherer = Gatherer.ofSequential(
            () -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in initializer"));
                return new int[]{0};
            },
            (int[] state, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                state[0] += x;
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in integrator for x=" + x + " with state[0]=" + state[0]));
                return result.push(state[0]);
            },
            (int[] state, Gatherer.Downstream<? super Integer> downstream) -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in finisher with state[0]=" + state[0]));
            });

    /*
    Gatherer with initializer, integrator, and finisher (v2).

    - Initializer: Creates a new `ArrayList<Integer>` to accumulate state.
    - Integrator: For each input `x`, adds `x` plus the last value in the state (or 0 if empty) to the state list.
      This effectively builds a running sum sequence.
      Logs the operation for each element processed.
    - Finisher: Pushes all accumulated values in the state to the downstream.
      Logs the final state.

    This gatherer processes all elements sequentially, accumulating a running sum in the state list.
    After all elements are processed, the finisher emits the entire sequence to the downstream in parallel.
 */
    static Gatherer<Integer, List<Integer>, Integer> initializerWithIntegratorAndFinisherSequenceGatherer_v2 = Gatherer.ofSequential(
            () -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in initializer"));
                return new ArrayList<Integer>();
            },
            (List<Integer> state, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                state.add(x + (state.isEmpty() ? 0 : state.getLast()));
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in integrator for x=" + x + " with state=" + state));
                return true;
            },
            (List<Integer> state, Gatherer.Downstream<? super Integer> downstream) -> {
                state.forEach(downstream::push);
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in finisher with state=" + state));
            });

    /*
    Test: parallelStreamWithSequenceGatherer1

    This test verifies the behavior of a parallel stream using a sequence gatherer with only an integrator.

    - Input: List of integers [1, 2, 3, 4, 5] processed as a parallel stream.
    - Processing steps:
        1. Filter elements greater than zero.
        2. Map each element by multiplying by two.
        3. Gather using `onlyIntegratorSequenceGatherer` (forces sequential processing up to this point).
        4. Map each gathered element by multiplying by three.
        5. Filter elements less than one hundred.
        6. Limit to the first three elements.
        7. Collect results to a list.

    - Expected output: [6, 12, 18]
      (corresponds to: ((1*2)*3), ((2*2)*3), ((3*2)*3))

    The test asserts that the output list matches the expected values in both elements and order.
 */
    @Test
    void parallelStreamWithSequenceGatherer1() {
        System.out.println("Processing test: parallelStreamWithSequenceGatherer()");
        System.out.println("Invoking stream processing: parallel stream with sequence gatherer with only integrator");
        var output1 = inputList
                .parallelStream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(onlyIntegratorSequenceGatherer)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();

        System.out.println(output1);
        assertEquals(List.of(6, 12, 18), output1, "Lists should be equal in both elements and order");
    }

    /**
     * Test: parallelStreamWithSequenceGatherer2
     *
     * Verifies the behavior of a parallel stream using a sequence gatherer with integrator and finisher.
     *
     * Steps:
     * 1. Filters elements greater than zero.
     * 2. Maps each element by multiplying by two.
     * 3. Gathers using integratorWithFinisherSequenceGatherer (forces sequential processing up to this point).
     * 4. Maps each gathered element by multiplying by three.
     * 5. Filters elements less than one hundred.
     * 6. Limits to the first three elements.
     * 7. Collects results to a list.
     *
     * Expected output: [6, 12, 18]
     * (corresponds to: ((1*2)*3), ((2*2)*3), ((3*2)*3))
     */
    @Test
    void parallelStreamWithSequenceGatherer2() {
        System.out.println("Invoking stream processing: parallel stream with sequence gatherer with integrator & finisher");
        var output2 = inputList
                .parallelStream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(integratorWithFinisherSequenceGatherer)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();
        System.out.println(output2);
        assertEquals(List.of(6, 12, 18), output2, "Lists should be equal in both elements and order");
    }

    @Test
    void parallelStreamWithSequenceGatherer3() {
        System.out.println("Invoking stream processing: parallel stream with sequence gatherer with initializer & integrator");
        var output3 = inputList
                .parallelStream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(initializerWithIntegratorSequenceGatherer)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();
        System.out.println(output3);
        assertEquals(List.of(6, 18, 36), output3, "Lists should be equal in both elements and order");
    }

    @Test
    void parallelStreamWithSequenceGatherer4() {

        System.out.println("Invoking stream processing: parallel stream with sequence gatherer with initializer & integrator & finisher");
        var output4 = inputList
                .parallelStream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(initializerWithIntegratorAndFinisherSequenceGatherer)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();
        System.out.println(output4);
        assertEquals(List.of(6, 18, 36), output4, "Lists should be equal in both elements and order");
    }

    @Test
    void parallelStreamWithSequenceGatherer5() {
        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with initializer & integrator & finisher v2");
        var output5 = inputList
                .parallelStream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(initializerWithIntegratorAndFinisherSequenceGatherer_v2)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();
        System.out.println(output5);
        assertEquals(List.of(6, 18, 36), output5, "Lists should be equal in both elements and order");
    }
}
