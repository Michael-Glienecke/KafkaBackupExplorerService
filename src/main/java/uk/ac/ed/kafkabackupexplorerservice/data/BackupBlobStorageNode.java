package uk.ac.ed.kafkabackupexplorerservice.data;

import com.fasterxml.jackson.databind.deser.DataFormatReaders;
import io.swagger.models.auth.In;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private boolean didPatternMatch;
    @Setter
    private String fileContent;

    /**
     * Check for directory
     * @return if this is a directory node
     */
    public boolean isDirectoryNode() {
        return children != null && !children.isEmpty();
    }

    /**
     * check for file nodes
     * @return true if this is a child node
     */
    public boolean isFileNode() {
        return children == null || children.isEmpty();
    }

    /**
     * check if this is a file node with (!) a matching pattern to be a data file
     * @return true if it is a data file matching the pattern
     */
    public boolean isBackupDataFileNode() {
        return isFileNode() && didPatternMatch;
    }

    public BackupBlobStorageNode(String name, List<BackupBlobStorageNode> children) {
        this.children = children;
        this.name = name;
    }

    public BackupBlobStorageNode(String name, Pattern parsingPattern) {
        this.name = name;

        Matcher matcher = parsingPattern.matcher(name);
        this.didPatternMatch = matcher.find();
        if (didPatternMatch) {
            try {
                this.topicName =  matcher.group(2);
                this.fileName = matcher.group(7);
                this.representedDateTime = LocalDateTime.of(Integer.parseInt(matcher.group(3)), Integer.parseInt(matcher.group(4)), Integer.parseInt(matcher.group(5)), Integer.parseInt(matcher.group(6)), 0, 0);
            } catch (Exception ex) {
                log.error("error while parsing: " + name, ex);
                didPatternMatch = false;
                representedDateTime = LocalDateTime.MIN;
            }
        }
    }

    public BackupBlobStorageNode() {

    }
}
