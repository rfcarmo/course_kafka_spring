package com.learnkafka.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.domain.LibraryEvent;
import com.learnkafka.producer.LibraryEventsProducer;
import com.learnkafka.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author rfort
 **/
@WebMvcTest(LibraryEventsController.class)
class LibraryEventsControllerUnitTest {

    @Autowired
    MockMvc mockMvc;

    @Autowired
    ObjectMapper objectMapper;

    @MockitoBean
    LibraryEventsProducer libraryEventsProducer;

    @Test
    void postLibraryEvent() throws Exception {
        // given
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecord());

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

        // when
        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().isCreated());

        // then
    }

    @Test
    void postLibraryEvent_invalidValues() throws Exception {
        // given
        var json = objectMapper.writeValueAsString(TestUtil.libraryEventRecordWithInvalidBook());

        when(libraryEventsProducer.sendLibraryEvent_approach3(isA(LibraryEvent.class))).thenReturn(null);

        // when
        mockMvc.perform(MockMvcRequestBuilders.post("/v1/libraryevent")
                .content(json)
                .contentType(MediaType.APPLICATION_JSON)).andExpect(status().is4xxClientError());

        // then
    }
}