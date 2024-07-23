package com.globalshares.kafkabackupexplorerservice.controller;

import com.azure.core.annotation.QueryParam;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.globalshares.kafkabackupexplorerservice.data.BackupBlobStorageNode;
import io.swagger.v3.oas.annotations.Operation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.time.LocalDateTime;
import java.time.YearMonth;
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

    /**
     * the match for all topics
     */
    public static final String[] TOPIC_ALL = { "*", "ALL" };

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
     * list the entire tree of items. If no searchPattern is present, then only the structure (directory tree) is returned.
     * If a pattern (in RegEx format) is given, then all matching nodes will be in the result
     *
     * @param nodesFrom defines the date nodes should start at
     * @param nodesUntil defines the date nodes should end at
     * @param topics is a list of comma (,) separated topics or * or ALL for all topics
     * @param searchPattern defines the search pattern in RegEx format to search (query parameter). If not present, then no data nodes will be returned
     * @return a list of tree entries (and file nodes, if search pattern matches were present)
     * @throws IOException should either the Azure connection fail or a security problem exist
     */
    @Operation
    @GetMapping(value = {"/", "/{topics}", "/{topics}/{nodesFrom}", "/{topics}/{nodesFrom}/{nodesUntil}" })
    public ResponseEntity<List<BackupBlobStorageNode>> getAllNodes(
            @PathVariable (required = false) String[] topics,
            @PathVariable (required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Optional<LocalDateTime> nodesFrom,
            @PathVariable (required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Optional<LocalDateTime> nodesUntil,
            @QueryParam("searchPattern")  Optional<String> searchPattern
            ) throws IOException {

        try {
            if (topics == null) {
                topics = new String[1];
                topics[0] = TOPIC_ALL[0];
            }

            // just checking...
            if (! nodesFrom.orElse(LocalDateTime.MIN).isBefore(nodesUntil.orElse(LocalDateTime.MAX)) || topics.length == 0) {
                log.debug(String.format("invalid parameters: From (%s), Until(%s), Topics(%s)", nodesFrom.orElse(LocalDateTime.MIN), nodesUntil.orElse(LocalDateTime.MAX), String.join(", ", topics)));
                return ResponseEntity.notFound().build();
            }

            Pattern searchPatternToUse = null;
            if (searchPattern.isPresent()) {
                searchPatternToUse = Pattern.compile(searchPattern.get());
            }

            oneTimeInit();
            var blobContainerClient = getBlobContainerClient(getBlobServiceClient());
            return ResponseEntity.ok(loadStorageNodes(blobContainerClient, kafkaBackupStorageContainerRootDirectory, topics, nodesFrom, nodesUntil, searchPatternToUse));
        } catch (Exception ex) {
            log.error("error during retrieving the backup tree", ex);
            throw ex;
        }
    }

    // Check if the passed filter equals a logical "all" topics
    private boolean useAllTopics(String[] filterTopics) {
        assert filterTopics != null;

        return filterTopics.length == 1 && Arrays.stream(TOPIC_ALL).anyMatch (t -> t.equalsIgnoreCase(filterTopics[0]));
    }

    private List<BackupBlobStorageNode> loadStorageNodes(
            BlobContainerClient client,
            String rootNode,
            String[] filterTopics,
            Optional<LocalDateTime> nodesFrom,
            Optional<LocalDateTime> nodesUntil,
            Pattern searchPattern) {
        var nodes = new ArrayList<BackupBlobStorageNode>();

        client.listBlobsByHierarchy(rootNode).forEach(blob -> {
            log.debug(String.format("%s %s%n", (blob.isPrefix() ? "(D)" : "   "), blob.getName()));

            BackupBlobStorageNode currentNode = null;

            if (blob.isPrefix()) {
                // check for the dates in structural nodes as perhaps no traversal is necessary
                Matcher matcher = structuralNodePattern.matcher(blob.getName());

                // parse the name -> first in order will be the year, then the month, day and hour
                if (! matcher.matches()) {
                    // should there be no match... problem...
                    log.debug(String.format("a directory node doesn't match the pattern and was skipped: " + blob.getName()));
                    return;
                }

                // build the comparison map of the real compare values
                Map<String, String> keyValueSet = new HashMap<>();
                matcher.namedGroups().forEach((groupName, index) -> {
                    log.trace(String.format("%s (%d): %s%n", groupName, index, matcher.group(groupName)));

                    if (matcher.group(groupName) != null) {
                        keyValueSet.put(groupName, matcher.group(groupName));
                    }
                });

                boolean isNodeUsable = isNodeUsable(blob, keyValueSet, filterTopics, nodesFrom.orElse(LocalDateTime.MIN), nodesUntil.orElse(LocalDateTime.MAX));
                if (! isNodeUsable) {
                    log.debug(String.format("Node %s skipped as date range was set to %s - %s%n", blob.getName(), nodesFrom.orElse(LocalDateTime.MIN), nodesUntil.orElse(LocalDateTime.MAX)));
                    return;
                }

                // only use the record if the match is correct. If no data is passed in then MIN / MAX will be used (always match). In case only the topic is passed then just continue
                currentNode = new BackupBlobStorageNode(blob.getName(), loadStorageNodes(client, blob.getName(), filterTopics, nodesFrom, nodesUntil, searchPattern));
            } else {
                // only if we are searching for something really
                if (searchPattern != null) {

                    // then we check for the files
                    Matcher matcher = fileNodePattern.matcher(blob.getName());
                    if (matcher.matches()) {
                        try {
                            // if it is a proper data file, then check the content if a match happens
                            String uncompressedFileContent = decompressDataFile(client, blob);
                            if (searchPattern.matcher(uncompressedFileContent).find()) {
                                String topicName =  matcher.group("topic");
                                String fileName = matcher.group("fileName");
                                LocalDateTime representedDateTime = LocalDateTime.of(Integer.parseInt(matcher.group("year")), Integer.parseInt(matcher.group("month")), Integer.parseInt(matcher.group("day")), Integer.parseInt(matcher.group("hour")), 0, 0);

                                currentNode = new BackupBlobStorageNode(blob.getName(), topicName, fileName, representedDateTime, uncompressedFileContent);

                                log.debug(String.format("Topic: %s, represented time: %s, filename: %s\n", currentNode.getTopicName(), currentNode.getRepresentedDateTime().toString(), currentNode.getFileName()));
                                log.debug(String.format("First 400 characters: %s\n", currentNode.getFileContent().substring(0, Math.min(currentNode.getFileContent().length(), 400))));
                            } else {
                                log.debug(String.format("entry skipped as content does not match pattern: %s ", searchPattern.pattern()));
                            }
                        } catch (Exception ex) {
                            log.error(String.format("error while parsing blob item: %s %s ", blob.getName(), ex));
                        }
                    } else {
                        // if no match for the filename pattern then this is any other file which we do not want to use
                        log.debug("entry skipped as no proper data file");
                    }
                } else {
                    log.debug("as no search pattern is present no need to scan files as well");
                }
            }

            if (currentNode != null) {
                nodes.add(currentNode);
            }
        });

        return nodes;
    }

    /**
     * decompress the content (in gz format) of the blob item
     * @param client is the blob container client
     * @param blob is the item to decompress
     * @return the decompressed string
     */
    private String decompressDataFile(BlobContainerClient client, BlobItem blob) {
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

            return fos.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isNodeUsable(BlobItem blob, Map<String, String> keyValueSet, String[] filterTopics, LocalDateTime from, LocalDateTime until) {

        // now we can check for the topic(s) - if not all, then there must be a match
        if (! useAllTopics(filterTopics) && Arrays.stream(filterTopics).noneMatch(t -> keyValueSet.get("topic").equalsIgnoreCase(t))) {
            log.debug(String.format("Node %s skipped as topic %s was not in the filtered topic list: %s%n", blob.getName(), keyValueSet.get("topic"), String.join(", ", filterTopics)));
            return false;
        }

        int day = 1;
        int month = 1;
        int year = 1900;
        int hour = 0;
        boolean isNodeUsable = true;

        for (String key : keyValueSet.keySet()) {
            switch (key) {
                case "year":
                    year = Integer.parseInt(keyValueSet.get(key));
                    if (from.getYear() > year || until.getYear() < year) {
                        log.debug(String.format("Year %d is not in range", year));
                        isNodeUsable = false;
                    }
                    break;

                case "month":
                    month = Integer.parseInt(keyValueSet.get(key));
                    if (
                        ! from.isBefore(LocalDateTime.of(year, month, YearMonth.of(year, month).lengthOfMonth(), 23, 59, 59)) ||
                        ! until.isAfter(LocalDateTime.of(year, month, 1, 0, 0, 0)))
                    {
                        log.debug(String.format("%d-%d is not in range", year, month));
                        isNodeUsable = false;
                    }
                    break;

                case "day":
                    day = Integer.parseInt(keyValueSet.get(key));
                    if (
                        ! from.isBefore(LocalDateTime.of(year, month, day, 23, 59, 59)) ||
                        ! until.isAfter(LocalDateTime.of(year, month, day, 0, 0, 0)))
                    {
                        log.debug(String.format("%d-%d-%d is not in range", year, month, day));
                        isNodeUsable = false;
                    }
                    break;

                case "hour":
                    hour = Integer.parseInt(keyValueSet.get(key));
                    if (
                        ! from.isBefore(LocalDateTime.of(year, month, day, hour, 59, 59)) ||
                        ! until.isAfter(LocalDateTime.of(year, month, day, hour, 0, 0)))
                    {
                        log.debug(String.format("%d-%d-%d %d is not in range", year, month, day, hour));
                        isNodeUsable = false;
                    }
                    break;

            }

            if (! isNodeUsable) {
                break;
            }
        }

        return isNodeUsable;
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
