package org.example.todelete;

import jdk.jfr.Description;
import org.junit.jupiter.api.Test;

import static org.example.util.Utils.constructMessage;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.*;

class GatherersTest {
    static List<Integer> inputList = List.of(1,2,3,4,5, 6, 7, 8, 9, 10);

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
            (Void _,  Integer x,  Gatherer.Downstream<? super Integer> result) -> {
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
            (Void _,  Integer x,  Gatherer.Downstream<? super Integer> result) -> {
                System.out.println(constructMessage("in gatherer with only integrator for x=" + x));
                return result.push(x);
            },
            (Void _, Gatherer.Downstream<? super Integer> result) -> {
                result.push(11);
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
            (int[] state,  Integer x,  Gatherer.Downstream<? super Integer> result) -> {
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
            (int[] state,  Integer x,  Gatherer.Downstream<? super Integer> result) -> {
                state[0] += x;
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in integrator for x=" + x + " with state[0]=" + state[0]));
                return result.push(state[0]);
            },
            (int[] state, Gatherer.Downstream<? super Integer> downstream) -> {
                System.out.println(constructMessage("in gatherer with initializer & integrator & finisher in finisher with state[0]=" + state[0]));
            });
    /*
        Input: Sequence Stream
        Gatherer: Sequence
        Processing: all elements - upstream, gatherer and post gatherer stream processing is done sequentially.
        i.e all elements flow through from beginning to end one element at a time. no blocking or no parallel processing.
     */
    @Test
    void sequenceStreamWithSequenceGatherer() {
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

    /*
       Input: Parallel Stream
       Gatherer: Sequence
       Processing: all elements till the gatherer (which is sequential) run in sequential though the upstream is parallel.
       Post gatherer, the stream becomes parallel.
       i.e all elements till gatherer run in sequential (gatherer becomes more like a blocking), and from there it become parallel.
    */
    @Test
    void parallelStreamWithSequenceGatherer() {
        System.out.println("Processing test: parallelStreamWithSequenceGatherer()");

        Gatherer<Integer, Void, Integer> doingNothingGatherer = Gatherer.ofSequential(
                (Void _,  Integer x,  Gatherer.Downstream<? super Integer> result) -> {
                    System.out.println(constructMessage("in gatherer for x=" + x));
                    return result.push(x);
                });

        var output = inputList
                .parallelStream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(doingNothingGatherer)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();

        System.out.println(output);
    }

    /*
        Input: Parallel Stream
        Gatherer: Parallel
        Processing:
     */
    @Test
    void parallelStreamWithParallelGatherer() {
        System.out.println("Processing test: parallelStreamWithParallelGatherer()");
        /*
        Gatherer<Integer, int[], Integer> doingNothingGatherer1 = Gatherer.of(
                () -> {
                    System.out.println(constructMessage("in gatherer initializer"));
                    return new int[]{0};
                },
                (int[] state,  Integer x,  Gatherer.Downstream<? super Integer> result) -> {
                    System.out.println(constructMessage("in gatherer integrator for x=" + x + ", with state:" + state[0]));
                    state[0] += x;
                    return true;
                },
                (int[] left, int[] right) -> {
                    System.out.println(constructMessage("in gatherer combiner for left=" + left + ", right=" + right));
                    left[0] += right[0];
                    return left;
                },
                (int[] state, Gatherer.Downstream<? super Integer> downstream) -> downstream.push(state[0]));
        */

        Gatherer<Integer, List<Integer>, Integer> runningTotalGatherer = Gatherer.of(
                () -> {
                    System.out.println(constructMessage("in gatherer initializer"));
                    return new ArrayList<>();
                },
                (List<Integer> state,  Integer x,  Gatherer.Downstream<? super Integer> result) -> {
                    System.out.println(constructMessage("in gatherer integrator for x=" + x + ", with state:" + state));
                    return state.add( (state.isEmpty() ? 0 : state.getLast()) + x);
                },
                (List<Integer> left, List<Integer> right) -> {
                    System.out.println(constructMessage("in gatherer combiner for left=" + left + ", right=" + right));
                    Integer prevLastValue = left.getLast();
                    left.addAll(right.stream().map(element -> element + prevLastValue).toList());
                    return left;
                },
                (List<Integer> state, Gatherer.Downstream<? super Integer> downstream) -> {
                    System.out.println(constructMessage("in gatherer finisher for state=" + state));
                    state.forEach(downstream::push);
                });

        var output = inputList
                .parallelStream()
                .filter(greaterThanZero)
                .map(multiplyByTwo)
                .gather(runningTotalGatherer)
                .map(multiplyByThree)
                .filter(lessThanHundred)
                .limit(3)
                .toList();

        System.out.println(output);
    }

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
List.copyOf(inputList);

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
                            +" old state:" + oldState + " , state:" + state + ", element:" + element + ", downstream:" + downstream);

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
        List.of(1,2,3,6,7,8,10)
                .parallelStream()
                .gather(Gatherer.<Integer, boolean[], Integer>of(
                        () -> new boolean[]{true},
                        (state, element, downStram) -> {
//                            try { Thread.sleep(100 * element); } catch(Exception e) {}

//                            System.out.println("In Integrator: " + Thread.currentThread().getName()
//                                    + " , state:" + state[0] + ", element:" + element );
                            if(state[0] == false) return false;

                            if(element >= 5){
                                state[0] = false;
                                return state[0];
                            }

                            downStram.push(element);
                            return true;
                        },
                        (left, right) -> left ,
                        (state, downStream) -> {}
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
