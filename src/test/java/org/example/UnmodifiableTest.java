package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UnmodifiableTest {
    @Test
    void test1(){
        List<String> orgList = new ArrayList<>(List.of("cat", "ball", "apple"));
        var list1 = Collections.unmodifiableList(orgList);
        list1.forEach(System.out::println);
        assertThrows(UnsupportedOperationException.class,  () -> list1.addAll(List.of("new element")));
        System.out.println("After adding new element");
        orgList.add("zebra");
//        orgList.forEach(System.out::println);
        list1.forEach(System.out::println);
    }

    @Test
    void test2(){
        var list1 = List.of(1,2,3);
        System.out.println("class:" + list1.getClass());
//        list1.addAll(List.of(10,11));
        assertThrows(UnsupportedOperationException.class,  () -> list1.addAll(List.of(10,11)));
    }
}
