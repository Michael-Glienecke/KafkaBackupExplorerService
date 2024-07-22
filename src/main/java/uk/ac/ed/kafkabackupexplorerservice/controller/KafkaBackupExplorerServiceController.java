package uk.ac.ed.kafkabackupexplorerservice.controller;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.kafkabackupexplorerservice.data.BackupBlobStorageNode;

import java.io.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;


/**
 * the ILP Tutorial service which provides suppliers, orders and other useful things
 */
@Slf4j
@RestController()
@RequestMapping("/api/kafkabackupexplorer/v1")
public class KafkaBackupExplorerServiceController {

    public static final String KAFKA_BACKUP_STORAGE_CONNECTION_STRING = "KAFKA_BACKUP_STORAGE_CONNECTION_STRING";
    public static final String KAFKA_BACKUP_CONTAINER_NAME = "KAFKA_BACKUP_CONTAINER_NAME";

    @Value("${kafka.backupStorage.connectionString}")
    private String kafkaBackupStorageConnectionString;

    @Value("${kafka.backupStorage.container.name}")
    private String kafkaBackupStorageContainer;

    @Value("${kafka.backupStorage.container.rootDirectory}")
    private String kafkaBackupStorageContainerRootDirectory;

    @Value("${debug.showOutput}")
    private boolean showDebugOutput;

    @Value("${regex.fileNode}")
    private String fileNodePatternRegEx;

    @Value("${regex.structuralNode}")
    private String structuralNodePatternRegEx;
    private Pattern fileNodePattern = null;
    private Pattern structuralNodePattern = null;


    /**
     * a simple alive check
     *
     * @return true (always)
     */
    @GetMapping(value = {"/isAlive"})
    public boolean isAlive() {
        return true;
    }

    private void oneTimeInit() {
        if (fileNodePattern == null) {
            fileNodePattern = Pattern.compile(fileNodePatternRegEx);
        }
        if (structuralNodePattern == null) {
            structuralNodePattern = Pattern.compile(structuralNodePatternRegEx);
            // structuralNodePattern = Pattern.compile("([a-zA-Z_0-9-]*)/?([a-zA-Z_0-9-]*)?/?(year=(\\d+))?/?(month=(\\d+))?/?(day=(\\d+))?/?(hour=(\\d+))?/");
        }
    }

    /**
     * list the entire tree of items
     * @return a list of tree entries always with / notation
     * @throws IOException should either the Azure connection fail or a security problem exist
     */
    @Operation
    @GetMapping(value = {"/", "/{nodesFrom}", "/{nodesFrom}/{nodesUntil}" })
    public ResponseEntity<List<BackupBlobStorageNode>> getAllNodes(
            @PathVariable (required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Optional<LocalDateTime> nodesFrom,
            @PathVariable (required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Optional<LocalDateTime> nodesUntil
            ) throws IOException {

        try {
            oneTimeInit();
            var blobContainerClient = getBlobContainerClient(getBlobServiceClient());
            return ResponseEntity.ok(loadStorageNodes(blobContainerClient, kafkaBackupStorageContainerRootDirectory, nodesFrom, nodesUntil));
        } catch (Exception ex) {
            log.error("error during retrieving the backup tree", ex);
            throw ex;
        }
    }

    private List<BackupBlobStorageNode> loadStorageNodes(BlobContainerClient client, String rootNode, Optional<LocalDateTime> nodesFrom, Optional<LocalDateTime> nodesUntil) {
        var nodes = new ArrayList<BackupBlobStorageNode>();

        client.listBlobsByHierarchy(rootNode).forEach(blob -> {

            if (showDebugOutput) {
                System.out.printf("%s %s%n", (blob.isPrefix() ? "(D)" : "   "), blob.getName());
            }

            BackupBlobStorageNode currentNode = null;

            if (!blob.isPrefix()) {
                currentNode = new BackupBlobStorageNode(blob.getName(), fileNodePattern);

                if (currentNode.isBackupDataFileNode()) {

                    try (ByteArrayOutputStream fos = new ByteArrayOutputStream()) {
                        try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(client.getBlobClient(blob.getName()).downloadContent().toBytes()))) {
                            byte[] buffer = new byte[1024];
                            int len;
                            while ((len = gis.read(buffer)) > 0) {
                                fos.write(buffer, 0, len);
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }

                        currentNode.setFileContent(fos.toString());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (showDebugOutput) {
                        System.out.printf("Topic: %s, represented time: %s, filename: %s\n", currentNode.getTopicName(), currentNode.getRepresentedDateTime().toString(), currentNode.getFileName());
                        System.out.printf("First 400 characters: %s\n", currentNode.getFileContent().substring(0, Math.min(currentNode.getFileContent().length(), 400)));
                    }
                }
            } else {
                // check for the dates in structural nodes as perhaps no traversal is necessary
                Matcher matcher = structuralNodePattern.matcher(blob.getName());

                // parse the name -> first in order will be the year, then the month, day and hour
                if (matcher.matches()) {

                    Map<String, String> keyValueSet = new HashMap<>();
                    matcher.namedGroups().forEach((groupName, index) -> {
                        if (showDebugOutput) {
                            System.out.printf("%s (%d): %s%n", groupName, index, matcher.group(groupName));
                        }

                        if (matcher.group(groupName) != null) {
                            keyValueSet.put(groupName, matcher.group(groupName));
                        }
                    });

                    // TODO has to be handled differently as only year might still be a valid match...
                    int day = 1;
                    int month = 1;
                    int year = 1900;
                    int hour = 0;
                    String topic = "";

                    for (String key : keyValueSet.keySet()) {
                        switch (key) {
                            case "day":
                                day = Integer.parseInt(keyValueSet.get(key));
                                break;

                            case "hour":
                                hour = Integer.parseInt(keyValueSet.get(key));
                                break;

                            case "year":
                                year = Integer.parseInt(keyValueSet.get(key));
                                break;

                            case "month":
                                month = Integer.parseInt(keyValueSet.get(key));
                                break;

                            case "topic":
                                topic = keyValueSet.get(key);
                        }
                    }

                    LocalDateTime matchingDateTime = LocalDateTime.of(year, month, day, hour, 0, 0);

                    // only use the record if the match is correct. If no data is passed in then MIN / MAX will be used (always match). In case only the topic is passed then just continue
                    boolean useRecord = keyValueSet.keySet().size() == 1 ||
                            matchingDateTime.isAfter(nodesFrom.orElse(LocalDateTime.MIN)) &&  matchingDateTime.isBefore(nodesUntil.orElse(LocalDateTime.MAX));

                    if (useRecord) {
                        currentNode = new BackupBlobStorageNode(blob.getName(), loadStorageNodes(client, blob.getName(), nodesFrom, nodesUntil));
                    } else {
                        if (showDebugOutput) {
                            System.out.printf("Node %s skipped as date range was set to %s - %s%n", blob.getName(), nodesFrom.orElse(LocalDateTime.MIN), nodesUntil.orElse(LocalDateTime.MAX));
                        }
                    }
                } else {
                    // should there be no match...
                    // problem... the entry doesn't match the pattern
                    log.error("a directory node doesn't match the pattern and was skipped: " + blob.getName());
                }
            }

            if (currentNode != null) {
                nodes.add(currentNode);
            }
        });

        return nodes;
    }


    private BlobClient getBlobClient(UUID uniqueId){
        var blobContainerClient = getBlobContainerClient(getBlobServiceClient());
        return blobContainerClient.getBlobClient(uniqueId.toString());
    }

    private BlobServiceClient getBlobServiceClient(){
        if (kafkaBackupStorageConnectionString == null || kafkaBackupStorageConnectionString.isBlank()) {
            throw new RuntimeException("Kafka backup storage connection string is not set");
        }
        return new BlobServiceClientBuilder().connectionString(kafkaBackupStorageConnectionString).buildClient();
    }

    private BlobContainerClient getBlobContainerClient(BlobServiceClient blobServiceClient) {
        if (kafkaBackupStorageContainer == null || kafkaBackupStorageContainer.isBlank()) {
            throw new RuntimeException("Kafka backup container is not set");
        }
        return blobServiceClient.getBlobContainerClient(kafkaBackupStorageContainer);
    }
}
