package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

/**
 * @author rfort
 **/
public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {
}
