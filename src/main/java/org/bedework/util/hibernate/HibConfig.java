/* ********************************************************************
    Appropriate copyright notice
*/
package org.bedework.util.hibernate;

import org.bedework.util.config.HibernateConfigBase;
import org.bedework.util.logging.BwLogger;
import org.bedework.util.logging.Logged;

import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

import java.io.File;
import java.io.StringReader;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/** Get configuration from JMX bean
 * User: mike
 * Date: 1/24/17
 * Time: 00:17
 */
public class HibConfig implements Logged {
  private final HibernateConfigBase<?> config;
  private final ClassLoader classLoader;
  
  public HibConfig(final HibernateConfigBase<?> config) {
    this.config = config;
    this.classLoader = config.getClass().getClassLoader();
  }

  public HibConfig(final HibernateConfigBase<?> config,
                   final ClassLoader classLoader) {
    this.config = config;
    this.classLoader = classLoader;
  }

  /**
   * @return Configuration based on the properties
   */
  public synchronized Configuration getHibConfiguration() {
    return getHibConfiguration("hibernate.cfg.xml");
  }

  /**
   * @param xmlCfgName name of config file - e.g "hibernate.cfg.xml"
   * @return Configuration based on the properties
   */
  public synchronized Configuration getHibConfiguration(final String xmlCfgName) {
    final URL url = classLoader.getResource(xmlCfgName);
    if (url == null) {
      throw new RuntimeException("Unable to locate " + xmlCfgName);
    }

    /*
    final File xmlCfgFile;
    try {
      xmlCfgFile = new File(url.toURI());
    } catch (final Throwable t) {
      // Always bad.
      error(t);

      throw new RuntimeException(t);
    }
*/
    return getHibConfiguration(url);
  }

  /**
   * @return Configuration based on the properties
   */
  public synchronized Configuration getHibConfiguration(final File xmlCfg) {
    try {
      final BootstrapServiceRegistry bsr =
              new BootstrapServiceRegistryBuilder().
                      applyClassLoader(classLoader).
                      build();
      final Configuration hibCfg = new Configuration(bsr);

      hibCfg.addProperties(getProps()).configure(xmlCfg);

      return hibCfg;
    } catch (final Throwable t) {
      // Always bad.
      error(t);
      
      throw new RuntimeException(t);
    }
  }

  /**
   * @return Configuration based on the properties
   */
  public synchronized Configuration getHibConfiguration(final URL xmlCfgUrl) {
    try {
      final BootstrapServiceRegistry bsr =
              new BootstrapServiceRegistryBuilder()
                      .applyClassLoader(this.getClass().getClassLoader())
                      .applyClassLoader(classLoader)
                      .build();
      final Configuration hibCfg = new Configuration(bsr);

      hibCfg.addProperties(getProps()).configure(xmlCfgUrl);

      return hibCfg;
    } catch (final Throwable t) {
      // Always bad.
      error(t);

      throw new RuntimeException(t);
    }
  }

  private Properties getProps() {
    try {
      final StringBuilder sb = new StringBuilder();

      final List<String> ps = config.getHibernateProperties();

      for (final String p: ps) {
        sb.append(p);
        sb.append("\n");
      }

      final Properties hprops = new Properties();
      hprops.load(new StringReader(sb.toString()));

      return hprops;
    } catch (final Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /* ==============================================================
   *                   Logged methods
   * ============================================================== */

  private final BwLogger logger = new BwLogger();

  @Override
  public BwLogger getLogger() {
    if ((logger.getLoggedClass() == null) && (logger.getLoggedName() == null)) {
      logger.setLoggedClass(getClass());
    }

    return logger;
  }
}
