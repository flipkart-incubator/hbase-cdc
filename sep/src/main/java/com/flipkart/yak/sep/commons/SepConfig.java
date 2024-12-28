package com.flipkart.yak.sep.commons;

import java.util.Map;

/**
 * Collection of {@link CFConfig} representing collection of multiple replication config.
 * @param <T> specific implementation of replication config
 */
public class SepConfig<T extends CFConfig> {

  private Map<String, T> cfConfig;
  private boolean propogateMutation = false;
  private CBConfig cbConfig = new CBConfig();

  public boolean getPropogateMutation() {
    return propogateMutation;
  }

  public void setPropogateMutation(boolean propogateMutation) {
    this.propogateMutation = propogateMutation;
  }

  public CBConfig getCbConfig(){
    return cbConfig;
  }

  public void setCbConfig(CBConfig cbConfig){
    this.cbConfig = cbConfig;
  }

  public Map<String, T> getCfConfig() {
    return cfConfig;
  }

  public void setCfConfig(Map<String, T> cfConfig) {
    this.cfConfig = cfConfig;
  }

  @Override public String toString() {
    return "SepConfig{" + "cfConfig=" + cfConfig + '}';
  }
}
