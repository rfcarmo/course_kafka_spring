package com.learnkafka.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * @author rfort
 **/
public record Book(
        @NotNull
        Integer bookId,

        @NotBlank
        String bookName,

        @NotBlank
        String bookAuthor
) {
}
