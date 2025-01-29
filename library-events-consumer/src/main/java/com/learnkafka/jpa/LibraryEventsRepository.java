package com.learnkafka.jpa;

import com.learnkafka.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

/**
 * @author rfort
 **/
public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {
}
