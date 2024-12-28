package com.flipkart.yak.sep.commons;

import com.flipkart.yak.sep.filters.WALEditOriginWhitelist;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Models column-family replication config. Contains columnFamily details to replicate and topic mapping.
 * This prepares the base model for configuration, any implementation of {@link MessageQueueReplicationEndpoint}
 * should extend this.
 *
 * @author gokulvanan.v
 */
public class CFConfig {


  /**
   * Set of qualifiers i.e. column names to be allowed for replication.
   */
  private Set<String> whiteListedQualifier = new HashSet<>();

  /**
   * Mapping of qualifiers to topic name, this topic names should be fully qualified, and will be used as it is.
   */
  private Map<String, String> qualifierToTopicNameMap = new HashMap<>();

  /**
   * This topic name will be used if NO topic is specified for a whitelisted qualifier
   */
  private String defaultTopicName;

  /**
   * Used to filter based on source of edit.
   */
  private WALEditOriginWhitelist walEditOriginWhitelist = WALEditOriginWhitelist.ANY_ORIGIN;
  private Set<String> whitelistedOrigins = new HashSet<>();

  public Set<String> getWhiteListedQualifier() {
    return whiteListedQualifier;
  }

  public void setWhiteListedQualifier(Set<String> whiteListedQualifier) {
    this.whiteListedQualifier = whiteListedQualifier;
  }

  public Map<String, String> getQualifierToTopicNameMap() {
    return qualifierToTopicNameMap;
  }

  public void setQualifierToTopicNameMap(Map<String, String> qualifierToTopicNameMap) {
    this.qualifierToTopicNameMap = qualifierToTopicNameMap;
  }

  public String getDefaultTopicName() {
    return defaultTopicName;
  }

  public WALEditOriginWhitelist getWalEditOriginWhitelist() {
    return walEditOriginWhitelist;
  }

  public void setWalEditOriginWhitelist(WALEditOriginWhitelist walEditOriginWhitelist) {
    this.walEditOriginWhitelist = walEditOriginWhitelist;
  }

  public Set<String> getWhitelistedOrigins() {
    return whitelistedOrigins;
  }

  public void setWhitelistedOrigins(Set<String> whitelistedOrigins) {
    this.whitelistedOrigins = whitelistedOrigins;
  }

  @Override public String toString() {
    return "CFConfig{" + ", whiteListedQualifier=" + whiteListedQualifier
        + ", qualifierToTopicNameMap=" + qualifierToTopicNameMap + ", defaultTopicName='" + defaultTopicName + '\''
        + ", walEditOriginWhitelist=" + walEditOriginWhitelist + ", whitelistedOrigins=" + whitelistedOrigins + '}';
  }
}
