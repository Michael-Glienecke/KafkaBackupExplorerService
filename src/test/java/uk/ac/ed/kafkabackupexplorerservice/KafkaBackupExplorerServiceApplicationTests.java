package uk.ac.ed.kafkabackupexplorerservice;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import uk.ac.ed.kafkabackupexplorerservice.controller.KafkaBackupExplorerServiceController;

@SpringBootTest
class KafkaBackupExplorerServiceApplicationTests {

    @Autowired
    private KafkaBackupExplorerServiceController kafkaBackupExplorerServiceController;

    @Test
    void contextLoads() {
        Assertions.assertTrue(kafkaBackupExplorerServiceController != null);
    }
}
