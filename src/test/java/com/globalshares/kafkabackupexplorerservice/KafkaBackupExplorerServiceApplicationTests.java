package com.globalshares.kafkabackupexplorerservice;

import com.globalshares.kafkabackupexplorerservice.controller.KafkaBackupExplorerServiceController;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaBackupExplorerServiceApplicationTests {

    @Autowired
    private KafkaBackupExplorerServiceController kafkaBackupExplorerServiceController;

    @Test
    void contextLoads() {
        Assertions.assertTrue(kafkaBackupExplorerServiceController != null);
    }
}
