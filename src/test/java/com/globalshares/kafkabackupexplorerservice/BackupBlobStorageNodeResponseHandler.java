package com.globalshares.kafkabackupexplorerservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.util.StringUtils;
import com.globalshares.kafkabackupexplorerservice.data.BackupBlobStorageNode;

import java.util.Arrays;
import java.util.List;

import static org.springframework.test.util.AssertionErrors.*;

@Slf4j
@Getter
public class BackupBlobStorageNodeResponseHandler {
    public BackupBlobStorageNode[] blobNodes;

    public void processResult(MvcResult result) throws Exception {
        if (blobNodes != null) {
            return;
        }

        String json = result.getResponse().getContentAsString();

        if (!StringUtils.hasLength(json)) {
            throw new RuntimeException("no payload in JSON response");
        }

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        this.blobNodes = mapper.readValue(json, BackupBlobStorageNode[].class);
    }

    public ResultMatcher isNotEmpty() {
        return result -> {
            processResult(result);
            assertTrue("result is not empty", blobNodes != null && blobNodes.length > 0);
        };
    }

    public ResultMatcher isEmpty() {
        return result -> {
            processResult(result);
            assertTrue("result is empty", blobNodes == null || blobNodes.length == 0);
        };
    }

    public ResultMatcher doesContainDataFileNodes() {
        return result -> {
            processResult(result);
            assertTrue("data files nodes are present", blobNodes != null && areDataFileNodesPresent(Arrays.stream(blobNodes).toList()));
        };
    }

    public ResultMatcher doesNotContainDataFileNodes() {
        return result -> {
            processResult(result);
            assertTrue("no data files nodes are present", blobNodes == null || ! areDataFileNodesPresent(Arrays.stream(blobNodes).toList()));
        };
    }

    /**
     * walk the tree to find a data file node
     * @param rootNodes the list of root nodes
     * @return true if any node is a data file node
     */
    private boolean areDataFileNodesPresent(List<BackupBlobStorageNode> rootNodes) {
        boolean result = false;

        for (var currentNode : rootNodes) {
            if (currentNode.isDataFileNode()) {
                result = true;
            } else {
                result = areDataFileNodesPresent(currentNode.getChildren());
            }

            if (result) break;
        }

        return result;
    }
}
