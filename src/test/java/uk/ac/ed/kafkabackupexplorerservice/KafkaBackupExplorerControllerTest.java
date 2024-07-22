package uk.ac.ed.kafkabackupexplorerservice;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.springframework.test.web.servlet.result.StatusResultMatchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

@SpringBootTest
@AutoConfigureMockMvc
public class KafkaBackupExplorerControllerTest {

    @Autowired
    private MockMvc mockMvc;


    @Test
    void regexTest() {
        Pattern pattern = Pattern.compile("([a-zA-Z_0-9-]+)/([a-zA-Z_0-9-]+)/year=(\\d+)/month=(\\d+)/day=(\\d+)/hour=(\\d+)/([a-zA-Z_0-9-+.]+)");
        Matcher matcher = pattern.matcher("topics/neptunedb-security/year=2023/month=11/day=03/hour=15/neptunedb-security+1+0000000000.json.gz");

        if (matcher.find()) {
            System.out.printf("Topic: %s\n", matcher.group(2));
            System.out.printf("Year: %s\n", matcher.group(3));
            System.out.printf("Month: %s\n", matcher.group(4));
            System.out.printf("Day: %s\n", matcher.group(5));
            System.out.printf("Hour: %s\n", matcher.group(6));
            System.out.printf("File: %s\n", matcher.group(7));
        }
    }


    @Test
    void getTreeShouldReturnData() throws Exception {
        this.mockMvc.perform(get("/api/kafkabackupexplorer/v1/backupNodes")).andExpect(status().isOk()).andExpect(content().contentType(MediaType.APPLICATION_JSON));
    }
}
