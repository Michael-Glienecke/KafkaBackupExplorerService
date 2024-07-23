package com.globalshares.kafkabackupexplorerservice;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
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
    void regexTestStructuralNodes() {
        final Pattern structuralNodePattern = Pattern.compile("([a-zA-Z_0-9-]*)/?(?<topic>([a-zA-Z_0-9-]*))?/?(year=(?<year>\\d+))?/?(month=(?<month>\\d+))?/?(day=(?<day>\\d+))?/?(hour=(?<hour>\\d+))?/");
        // final Pattern structuralNodePattern = Pattern.compile("([a-zA-Z_0-9-]*)/?([a-zA-Z_0-9-]*)?/?(year=(\\d+))?/?(month=(\\d+))?/?(day=(\\d+))?/?(hour=(\\d+))?/");

        String[] checkEntries = {
                "topics/neptunedb-reports/",
                "topics/neptunedb-reports/year=2023/",
                "topics/neptunedb-reports/year=2023/month=11/",
                "topics/neptunedb-reports/year=2023/month=11/day=03/",
                "topics/neptunedb-reports/year=2023/month=11/day=03/hour=15/"
        };

        for (int i=0; i < checkEntries.length; i++) {
            Matcher matcher = structuralNodePattern.matcher(checkEntries[i]);
            if (matcher.matches()) {
                System.out.printf("%s\n", checkEntries[i]);

                for (int j = 0; j <= matcher.groupCount(); j++) {
                    if (matcher.group(j) == null) {
                        break;
                    }
                    System.out.printf("%d: %s%n", j, matcher.group(j));
                }
            }
            matcher.namedGroups().forEach((g, index) -> {
                System.out.printf("%s (%d): %s%n", g, index, matcher.group(g));
            });
        }
    }

    @Test
    void getTreeShouldReturnData() throws Exception {
        this.mockMvc.perform(get("/api/kafkabackupexplorer/v1/"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(backupBlobs().isNotEmpty());
    }

    @Test
    void getTreeWithLimitShouldReturnData() throws Exception {
        this.mockMvc.perform(get("/api/kafkabackupexplorer/v1/*/2023-11-03T14:00:15"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(backupBlobs().isNotEmpty());
    }

    @Test
    void getTreeWithUnknownTopicShouldNotReturnData() throws Exception {
        this.mockMvc.perform(get("/api/kafkabackupexplorer/v1/ThisTopicDoesNotExist"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(backupBlobs().isEmpty());
    }

    @Test
    void searchAllShouldReturnDataNodes() throws Exception {
        this.mockMvc.perform(get("/api/kafkabackupexplorer/v1/*?searchPattern=.*"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(backupBlobs().doesContainDataFileNodes());
    }

    @Test
    void searchAllWithNoMatchShouldReturnNoDataNodes() throws Exception {
        this.mockMvc.perform(get("/api/kafkabackupexplorer/v1/*?searchPattern=\\w*(Glienecke)\\w"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(backupBlobs().doesNotContainDataFileNodes());
    }

    @Test
    void searchAllWithRealMatchShouldReturnDataNodes() throws Exception {
        this.mockMvc.perform(get("/api/kafkabackupexplorer/v1/*?searchPattern=gs.event.db.datachange.neptune.actions"))
                .andExpect(status().isOk())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(backupBlobs().doesContainDataFileNodes());
    }


    private BackupBlobStorageNodeResponseHandler backupBlobs() {
        return new BackupBlobStorageNodeResponseHandler();
    }
}
