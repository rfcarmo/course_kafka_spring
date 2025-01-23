package com.learnkafka.domain;

/**
 * @author rfort
 **/
public record Book(
        Integer bookId,
        String bookName,
        String bookAuthor
) {
}
