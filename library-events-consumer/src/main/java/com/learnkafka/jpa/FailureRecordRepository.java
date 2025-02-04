package com.learnkafka.jpa;

import com.learnkafka.entity.FailureRecord;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

/**
 * @author rfort
 **/
public interface FailureRecordRepository extends CrudRepository<FailureRecord, Integer> {

    List<FailureRecord> findAllByStatus(String retry);
}
