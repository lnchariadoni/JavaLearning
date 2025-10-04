package org.example.GathererTests;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Gatherer;

import static org.example.util.Utils.constructMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GatherersSequenceStreamSequenceGathererTest {
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
        when only integrator exists, the behavior is similar to map(). ie. from upstream to all the way to terminal operation,
        the elements flow through one by one in sequence.
     */
    static Gatherer<Integer, Void, Integer> onlyIntegratorSequenceGatherer = Gatherer.ofSequential(
            (Void _, Integer x, Gatherer.Downstream<? super Integer> result) -> {
                System.out.println(constructMessage("in gatherer with only integrator for x=" + x));
                return result.push(x);
            });

    /*
        when only integrator exists, the behavior is similar to map(). ie. from upstream to all the way to terminal operation,
        the elements flow through one by one in sequence. Since we have finisher, when the limit 3 is reached, the finisher is called.
        As limit is reached, any elements additions to downstream will be ignored.
        If we dont have limit, then finisher is called after all elements are passed through the integrator. if any new element is added to downstream in finisher,
        then that added new element will be passed to downstream i.e in this example case "multiply by 3" and "checking <100".
     */
    static Gatherer<Integer, Void, Integer> integratorWithFinisherSequenceGatherer = Gatherer.ofSequential(
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
      This is similar to with only integrator. The only difference is that initializer is called once in case of sequence stream.
      The state is maintained in between calls to integrator.
      The stream processing is end to end i.e after gatherer, the pushed element is passed through to the downstream before next element is processed by upstream.
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
      This is similar with sequence gatherer with finisher. The finisher is called only once at the end, when the entire stream processing is complete.
      This also works similar to other sequence gatherer, where the elements are processed end to end.
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
      The difference between the other variant of initializer + integrator + finisher is that, we are not pushing any element to downstream in integrator.
      So finisher becomes a blocking call. all elements are collected in finisher and pushed to dowstream in the finisher.
      Only then the rest of the operations are performed.
      This to be seen as gatherer with blocking behavior. As finisher is blocking the downstream operations.
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

        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with initializer & integrator");
        var output3 = inputList
                .stream()
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
    void sequenceStreamWithSequenceGatherer4() {
        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with initializer & integrator & finisher");
        var output4 = inputList
                .stream()
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
    void sequenceStreamWithSequenceGatherer5() {
        System.out.println("Invoking stream processing: sequence stream with sequence gatherer with initializer & integrator & finisher v2");
        var output5 = inputList
                .stream()
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
