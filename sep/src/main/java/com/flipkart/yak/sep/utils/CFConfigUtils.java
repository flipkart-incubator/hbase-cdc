package com.flipkart.yak.sep.utils;

import com.flipkart.yak.sep.commons.CFConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Utility classes to parse {@link CFConfig}
 */
public class CFConfigUtils {


    private CFConfigUtils() {
    }

    private static final String TOPIC_PREFIX = "yak";
    private static final String TOPIC_DELIMETER = "_";

    public static final Logger logger = LoggerFactory.getLogger(CFConfigUtils.class);

    /**
     * This is called when column-qualifier/cell information is not required in propagation. Specifically, for mutation-wise propagation,
     * this method can be invoked.
     * @param tableName the table for which the replication is taking place
     * @param columnFamily the columnFamily present in the mutation
     * @param cfConfigOptional
     * @return Topic Name as per contract
     */
    public static Optional<String> getTopicNameFromConfig(TableName tableName, String columnFamily, Optional<CFConfig> cfConfigOptional)
    {
        String topic;
        String topicPrefix = new StringBuilder().append(TOPIC_PREFIX).append(TOPIC_DELIMETER).append(tableName.getNamespaceAsString())
                .append(TOPIC_DELIMETER).append(tableName.getQualifierAsString()).toString();
        CFConfig cfConf;
        if (!cfConfigOptional.isPresent()) {
            logger.debug("No cfConfig found");
            return Optional.empty();
        } else {
            cfConf = cfConfigOptional.get();
        }
        if(columnFamily != "default") {
            topic = new StringBuilder().append(topicPrefix).append(TOPIC_DELIMETER).append(columnFamily).toString().toLowerCase();
        } else {
            topic = topicPrefix.toLowerCase();
        }
        if (StringUtils.isNotBlank(cfConf.getDefaultTopicName())) {
            topic = cfConf.getDefaultTopicName();
            logger.debug("Changing topicName to {}", topic);
        }
        return  Optional.of(topic);
    }

    /**
     * In absence of qualifierToTopicNameMap, the default topic Name chosen would be
     * yak_<namespace>_<table>_<columnfamily>_<qualifier>. Else get from {@link CFConfig}
     */
    public static Optional<String> getTopicNameFromConfig(TableName tableName, String columnFamily,
                                                    String qualifier, Optional<CFConfig> cfConfigOptional) {

        String topicPrefix =
                new StringBuilder().append(TOPIC_PREFIX).append(TOPIC_DELIMETER).append(tableName.getNamespaceAsString())
                        .append(TOPIC_DELIMETER).append(tableName.getQualifierAsString()).toString();

        CFConfig cfConf;
        if (!cfConfigOptional.isPresent()) {
            logger.debug("no cfConfig found");
            return Optional.empty();
        } else {
            cfConf = cfConfigOptional.get();
        }

        if (!cfConf.getWhiteListedQualifier().isEmpty() && !cfConf.getWhiteListedQualifier()
                .contains(qualifier)) {
            logger.debug("ignoring not whitelisted qualifier");
            return Optional.empty();
        }

        String topic =
                new StringBuilder().append(topicPrefix).append("_").append(columnFamily).append("_")
                        .append(qualifier).toString().toLowerCase();

        if (StringUtils.isNotBlank(cfConf.getDefaultTopicName())) {
            topic = cfConf.getDefaultTopicName();
            logger.debug("Changing topicName for column family {} to {}", columnFamily, topic);
        }

        if (cfConf.getQualifierToTopicNameMap().containsKey(qualifier)) {
            // override topicName as per config
            topic = cfConf.getQualifierToTopicNameMap().get(qualifier);
            logger.debug("Changing topicName for qualifier {} to {}", qualifier, topic);
        }
        return Optional.of(topic);
    }

}
