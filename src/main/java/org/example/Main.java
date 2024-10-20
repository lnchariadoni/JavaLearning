package org.example;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class Main {

    public static void Stream1(){
        String[] arr = new String[]{"a", "b", "c"};
        Arrays.stream(arr).forEach(System.out::println);
        Stream
                .of("a", "b", "c")
                .parallel()
                .forEach(
                e ->
                    System.out.println(Thread.currentThread().getName() + ": " + e)
                );

    }

    public static void main(String[] args) {
        System.out.println("Hello world!");
        Stream1();
    }
}