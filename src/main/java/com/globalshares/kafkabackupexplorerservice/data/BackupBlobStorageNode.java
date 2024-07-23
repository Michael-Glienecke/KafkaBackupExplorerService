package com.globalshares.kafkabackupexplorerservice.data;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.LocalDateTime;
import java.util.List;

/**
 * defines the storage nodes in the backup store
 */
@Getter
@Slf4j
public class BackupBlobStorageNode {

    private String name;
    private List<BackupBlobStorageNode> children;
    private LocalDateTime representedDateTime;
    private String topicName;
    private String fileName;
    private String fileContent;

    /**
     * Check for directory
     * @return if this is a directory node
     */
    @JsonIgnore
    public boolean isDirectoryNode() {
        return children != null && !children.isEmpty();
    }

    @JsonIgnore
    public boolean isDataFileNode() {
        return (children == null || children.isEmpty()) && fileContent != null && representedDateTime != null;
    }

    public BackupBlobStorageNode(String name, List<BackupBlobStorageNode> children) {
        this(name);
        this.children = children;
    }

    public BackupBlobStorageNode(String name, String topic, String fileName, LocalDateTime representedDateTime, String uncompressedContent) {
        this(name);

        this.topicName = topic;
        this.fileName = fileName;
        this.representedDateTime = representedDateTime;
        this.fileContent = uncompressedContent;
    }

    public BackupBlobStorageNode(String name) {
        this();
        this.name = name;
    }

    public BackupBlobStorageNode() {
        ;
    }
}
