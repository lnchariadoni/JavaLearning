package org.example.GathererTests;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Gatherer;

import static org.example.util.Utils.constructMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GatherersSequenceStreamParallelGathererTest {
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
        This is similar to sequence stream and sequence gatherer with only integrator.
        All elements flow from upstream to all the way to downstream. gatherer integrator is just a pass through.
     */
    static Gatherer<Integer, Void, Integer> onlyIntegratorSequenceGatherer = Gatherer.of(
            (Void _, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                System.out.println(constructMessage("in gatherer with only integrator for x=" + x));
                return result.push(x);
            });

    /*
        This is exactly same as sequence stream and sequence gatherer.
     */
    static Gatherer<Integer, Void, Integer> integratorWithFinisherSequenceGatherer = Gatherer.of(
            (Void _, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                System.out.println(constructMessage("in gatherer with only integrator for x=" + x));
                return result.push(x);
            },
            (Void _, Gatherer.Downstream<? super Integer> result) -> {
                result.push(11); // intentionally adding, so that we can see it in the logs when the finisher is called.
                System.out.println(constructMessage("in gatherer finisher"));
            }
    );

    /*
      This is exactly as stream sequential and gatherer sequential. all the elements flow through all the way to downstream one by one.
      The combiner is never called, as the stream is sequential.
    */
    static Gatherer<Integer, int[], Integer> initializerWithIntegratorAndCombinerAndFinisherSequenceGatherer = Gatherer.of(
            () -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in initializer"));
                return new int[]{0};
            },
            (int[] state, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                state[0] += x;
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in integrator for x=" + x + " with state[0]=" + state[0]));
                return result.push(state[0]);
            },
            (int[] left, int[] right) -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in finisher with left[0]=" + left[0] + "right[0]=" + right[0]));
                left[0] += right[0];
                return left;
            },
            (int[] state, Gatherer.Downstream<? super Integer> downstream) -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in finisher with state[0]=" + state[0]));
            });

    /*
      As no element is pushed downstream in the integrator, no elements are processed immediately after integrator.
      i.e finisher acts as a blocker. we are collecting all processed elements in the integrator. then the elements are once pushed at an instance in finisher.
      the moment the finisher pushes elements to downstream, the rest of the downstream processes.
    */
    static Gatherer<Integer, List<Integer>, Integer> initializerWithIntegratorAndCombinerAndFinisherSequenceGatherer_v2 = Gatherer.of(
            () -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in initializer"));
                return new ArrayList<Integer>();
            },
            (List<Integer> state, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                state.add(x + (state.isEmpty() ? 0 : state.getLast()));
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in integrator for x=" + x + " with state=" + state));
                return true;
            },
            (List<Integer> left, List<Integer> right) -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in finisher with left[0]=" + left + "right[0]=" + right));
                left.addAll(right);
                return left;
            },
            (List<Integer> state, Gatherer.Downstream<? super Integer> downstream) -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in finisher with state=" + state));
                state.forEach(downstream::push);
            });

    /*
        Input: Sequence Stream
        Gatherer: Sequence
        Processing: all elements - upstream, gatherer and post gatherer stream processing is done sequentially.
        i.e all elements flow through from beginning to end one element at a time. no blocking or no parallel processing.
     */
    @Test
    void sequenceStreamWithSequenceGatherer1() {
        System.out.println("Processing test: sequenceStreamWithSequenceGatherer()");
        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with only integrator");
        var output1 = inputList
                .stream()
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

    @Test
    void sequenceStreamWithSequenceGatherer2() {
        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with integrator & finisher");
        var output2 = inputList
                .stream()
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
    void sequenceStreamWithSequenceGatherer3() {

        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with initializer & integrator & finisher");
        var output4 = inputList
                .stream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(initializerWithIntegratorAndCombinerAndFinisherSequenceGatherer)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();
        System.out.println(output4);
        assertEquals(List.of(6, 18, 36), output4, "Lists should be equal in both elements and order");
    }

    @Test
    void sequenceStreamWithSequenceGatherer4() {
        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with initializer & integrator & finisher v2");
        var output5 = inputList
                .stream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(initializerWithIntegratorAndCombinerAndFinisherSequenceGatherer_v2)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();
        System.out.println(output5);
        assertEquals(List.of(6, 18, 36), output5, "Lists should be equal in both elements and order");
    }
}
