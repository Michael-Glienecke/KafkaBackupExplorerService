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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

    private final Pattern fileNodePattern = Pattern.compile("([a-zA-Z_0-9-]+)/([a-zA-Z_0-9-]+)/year=(\\d+)/month=(\\d+)/day=(\\d+)/hour=(\\d+)/([a-zA-Z_0-9-+.]+)");

    /**
     * a simple alive check
     *
     * @return true (always)
     */
    @GetMapping(value = {"/isAlive"})
    public boolean isAlive() {
        return true;
    }

    /**
     * list the entire tree of items
     * @return a list of tree entries always with / notation
     * @throws IOException should either the Azure connection fail or a security problem exist
     */
    @Operation
    @GetMapping(value = {"/backupNodes", "/backupNodes/{nodesFrom}", "/backupNodes/{nodesFrom}/{nodesUntil}" })
    public ResponseEntity<List<BackupBlobStorageNode>> getBackupNodes(
            @PathVariable (required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Optional<LocalDateTime> nodesFrom,
            @PathVariable (required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Optional<LocalDateTime> nodesUntil
            ) throws IOException {

        try {
            var blobContainerClient = getBlobContainerClient(getBlobServiceClient());
            return ResponseEntity.ok(loadStorageNodes(blobContainerClient, kafkaBackupStorageContainerRootDirectory));
        } catch (Exception ex) {
            log.error("error during retrieving the backup tree", ex);
            throw ex;
        }
    }

    private List<BackupBlobStorageNode> loadStorageNodes(BlobContainerClient client, String rootNode) {
        var nodes = new ArrayList<BackupBlobStorageNode>();

        client.listBlobsByHierarchy(rootNode).forEach(blob -> {

            System.out.printf("%s %s%n", (blob.isPrefix() ? "(D)" : "   "), blob.getName());

            BackupBlobStorageNode currentNode = blob.isPrefix() ?
                new BackupBlobStorageNode(blob.getName(), loadStorageNodes(client, blob.getName())) :
                new BackupBlobStorageNode(blob.getName(), fileNodePattern);

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

                System.out.printf("Topic: %s, represented time: %s, filename: %s\n", currentNode.getTopicName(), currentNode.getRepresentedDateTime().toString(), currentNode.getFileName());
                System.out.printf("First 400 characters: %s\n", currentNode.getFileContent().substring(0, Math.min(currentNode.getFileContent().length(), 400)));
            }
            nodes.add(currentNode);
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
