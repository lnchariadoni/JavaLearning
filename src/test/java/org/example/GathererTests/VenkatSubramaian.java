package org.example.GathererTests;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Gatherer;
import java.util.stream.Gatherers;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class VenkatSubramaian {
  @Test
  void listFilterAndMap() {
    List<Integer> numbers = List.of(1);//, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

    numbers
        .stream()
        .parallel()
        .filter(e -> e % 2 == 0)
        .map(e -> e * 2)
        .findFirst()
        .ifPresentOrElse(System.out::println, () -> System.out.println("No value found"));
  }

  @Test
  void customOrderedPeek() {
    List<Integer> numbers = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

    numbers
        .stream()
        .parallel()
        .gather(customOrderedPeekGatherer(System.out::println))
        .forEach(e -> {
        });
  }

  private static Gatherer<Integer, Void, Integer> customOrderedPeekGatherer(
      Consumer<Integer> consumer) {
    return Gatherer.ofSequential(
        (_, element, downStream) ->
            consumeAndPush(consumer, element, downStream));
  }

  private static boolean consumeAndPush(Consumer<Integer> consumer,
                                        Integer element,
                                        Gatherer.Downstream<? super Integer> downStream) {
    consumer.accept(element);
    return downStream.push(element);
  }

  record NameWithIndex(String name, int index) {
  }

  @Test
  void mapWithIndexWithoutGatherer() {
    List<String> names = List.of("A", "B", "C", "D", "E");

    List<NameWithIndex> namesWithIndex = IntStream.
        range(0, names.size())
        .parallel() // to simulate parallel processing
        .mapToObj(i -> new NameWithIndex(names.get(i), i))
        .toList();

    List<NameWithIndex> expected = List.of(
        new NameWithIndex("A", 0),
        new NameWithIndex("B", 1),
        new NameWithIndex("C", 2),
        new NameWithIndex("D", 3),
        new NameWithIndex("E", 4)
    );

    Assertions.assertEquals(expected, namesWithIndex);
  }

  @Test
  void mapWithIndexWithGatherer() {
    List<String> names = List.of("A", "B", "C", "D", "E");

    List<NameWithIndex> namesWithIndex = names
        .stream()
        .parallel() // to simulate parallel processing
        .gather(consturctNamesWithIndexGatherer())
        .toList();

    List<NameWithIndex> expected = List.of(
        new NameWithIndex("A", 0),
        new NameWithIndex("B", 1),
        new NameWithIndex("C", 2),
        new NameWithIndex("D", 3),
        new NameWithIndex("E", 4)
    );

    Assertions.assertEquals(expected, namesWithIndex);
  }

  private static Gatherer<String, int[], NameWithIndex> consturctNamesWithIndexGatherer() {
    return Gatherer.ofSequential(
        () -> new int[] {0},
        //.ofSequential(
        (int[] index, String element, Gatherer.Downstream<? super NameWithIndex> downStream) ->
            downStream.push(new NameWithIndex(element, index[0]++))
    );
  }
}
