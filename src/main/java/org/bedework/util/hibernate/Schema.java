/* ********************************************************************
    Appropriate copyright notice
*/
package org.bedework.util.hibernate;

import org.bedework.util.jmx.InfoLines;

import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.cfg.Configuration;
import org.hibernate.tool.hbm2ddl.SchemaExport;
import org.hibernate.tool.schema.TargetType;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.EnumSet;

/** Handle the export of schemas
 * 
 * User: mike
 * Date: 1/23/17
 * Time: 22:45
 */
public class Schema {
  private Schema() {}

  /**
   * 
   * @return true if successful - false otherwise.
   */
  public static boolean execute(final InfoLines infoLines,
                                final String outFile,
                                final boolean export,
                                final HibConfig hibConfig) {
    try {
      infoLines.addLn("Started export of schema");

      final long startTime = System.currentTimeMillis();

      final SchemaExport se = new SchemaExport();
//      if (getDelimiter() != null) {
//        se.setDelimiter(getDelimiter());
//      }

      se.setDelimiter(";");
      se.setFormat(true);       // getFormat());
      se.setHaltOnError(false); // getHaltOnError());
      se.setOutputFile(outFile);
      se.setManageNamespaces(true);
        /* There appears to be a bug in the hibernate code. Everybody initialises
        this to /import.sql. Set to null causes an NPE
        Make sure it refers to a non-existant file */
      //se.setImportFile("not-a-file.sql");

      final EnumSet<TargetType> targets = EnumSet.noneOf(TargetType.class );

      if (export) {
        targets.add(TargetType.DATABASE);
      }

      if (outFile != null) {
        targets.add(TargetType.SCRIPT);
      }

      /*
      final BootstrapServiceRegistry bsr = 
              new BootstrapServiceRegistryBuilder().build();
      final StandardServiceRegistryBuilder ssrBuilder = 
              new StandardServiceRegistryBuilder(bsr);

      if (resourcePath == null) {
        ssrBuilder.configure();
      } else {
        ssrBuilder.configure(resourcePath);
      }
      ssrBuilder.applySettings(hibConfig);

      final StandardServiceRegistry ssr = ssrBuilder.build();
       */
      final Configuration cfg =
              hibConfig.getHibConfiguration("hibernate.cfg.xml"
              );
      final StandardServiceRegistryBuilder ssrBuilder =
              cfg.getStandardServiceRegistryBuilder();
      ssrBuilder.applySettings(cfg.getProperties());
      final StandardServiceRegistry ssr = ssrBuilder.build();

      se.execute(targets,
                 SchemaExport.Action.BOTH,
                 buildMetadata(ssr),
                 ssr);

      final long millis = System.currentTimeMillis() - startTime;
      long seconds = millis / 1000;
      final long minutes = seconds / 60;
      seconds -= (minutes * 60);

      infoLines.addLn("Elapsed time: " + minutes + ":" +
                              twoDigits(seconds));
      
      return true;
    } catch (final Throwable t) {
      final StringWriter sw = new StringWriter();
      t.printStackTrace(new PrintWriter(sw));
      
      infoLines.add(sw.toString());
      
      return false;
    } finally {
      infoLines.addLn("Schema build completed");
    }
  }

  private static MetadataImplementor buildMetadata(
          final StandardServiceRegistry serviceRegistry) {
    final MetadataSources metadataSources = new MetadataSources(serviceRegistry );

    //for ( String filename : parsedArgs.hbmXmlFiles ) {
    //  metadataSources.addFile( filename );
    //}

    //for ( String filename : parsedArgs.jarFiles ) {
    //  metadataSources.addJar( new File(filename ) );
    //}


    final MetadataBuilder metadataBuilder = metadataSources.getMetadataBuilder();
      /*
      final StrategySelector strategySelector = serviceRegistry.getService(StrategySelector.class );
      if ( parsedArgs.implicitNamingStrategyImplName != null ) {
        metadataBuilder.applyImplicitNamingStrategy(
                strategySelector.resolveStrategy(
                        ImplicitNamingStrategy.class,
                        parsedArgs.implicitNamingStrategyImplName
                )
        );
      }
      if ( parsedArgs.physicalNamingStrategyImplName != null ) {
        metadataBuilder.applyPhysicalNamingStrategy(
                strategySelector.resolveStrategy(
                        PhysicalNamingStrategy.class,
                        parsedArgs.physicalNamingStrategyImplName
                )
        );
      }
      */

    return (MetadataImplementor) metadataBuilder.build();
  }

  /**
   * @param val the number
   * @return 2 digit val
   */
  private static String twoDigits(final long val) {
    if (val < 10) {
      return "0" + val;
    }

    return String.valueOf(val);
  }
}
