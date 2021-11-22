package com.experiment.hiveserver2;


import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.server.HiveServer2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HiveLocalEmbeddedServer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(HiveLocalEmbeddedServer.class);
    private static final int DEFAULT_HIVE_SERVER2_PORT = 10001;
    private static final String BASE_DIR = "/temp/hive";
    public static final String DEFAULT_USER = "DrWho";
    public static final String DEFAULT_PASSWORD = "letMePass";

    private HiveServer2 hiveServer;
    private HiveConf config;
    private Integer port = getFreePort();
    private Boolean started;

    public HiveServer2 getHiveServer() {
        return hiveServer;
    }

    public HiveConf getConfig() {
        return config;
    }

    public Integer getPort() {
        return port;
    }

    public Boolean isStarted() {
        return started;
    }

    public void run() {
        try {
            start();
        } catch (Exception e) {
            LOGGER.error("HiveServer2 Failed To Start...", e);
            if (hiveServer != null) {
                stop();
            }
        } finally {
            LOGGER.error("Reached End Of Lifecycle...");
        }
    }

    private void start() throws Exception {
        LOGGER.info("Starting Hive Local/Embedded Server...");
        if (hiveServer == null) {
            config = configure();
            hiveServer = new HiveServer2();
            config.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
            hiveServer.init(config);
            hiveServer.start();
            waitForStartup();
            started = true;
        }
    }

    private int getFreePort() {
        try {
            port = MetaStoreUtils.findFreePort();
        } catch (Exception e) {
            port = DEFAULT_HIVE_SERVER2_PORT; // Default Hive Server port
        }
        LOGGER.info("Server Port: " + port);
        return port;
    }

    private void waitForStartup() throws Exception {
        long timeout = TimeUnit.MINUTES.toMillis(1);
        long unitOfWait = TimeUnit.SECONDS.toMillis(1);

        CLIService hs2Client = getServiceClientInternal();
        SessionHandle sessionHandle = null;
        for (int interval = 0; interval < timeout / unitOfWait; interval++) {
            Thread.sleep(unitOfWait);
            try {
                Map<String, String> sessionConf = Maps.newHashMap();
                sessionHandle = hs2Client.openSession(DEFAULT_USER, DEFAULT_PASSWORD, sessionConf);
                return;
            } catch (Exception e) {
                LOGGER.info("Error While Starting The Server. ", e);
            } finally {
                LOGGER.info("Closing Existing Sessions (if any)...");
                hs2Client.closeSession(sessionHandle);
            }
        }
        throw new TimeoutException("Couldn't get a hold of HiveServer2...");
    }

    private CLIService getServiceClientInternal() {
        for (Service service : hiveServer.getServices()) {
            if (service instanceof CLIService) {
                return (CLIService) service;
            }
        }
        throw new IllegalStateException("Cannot Find CLIService");
    }

    private HiveConf configure() throws Exception {
        LOGGER.info("Setting The Hive Conf Variables");
        String baseDir = BASE_DIR;

        Configuration configuration = new Configuration();
        HiveConf conf = new HiveConf(configuration, HiveConf.class);
        conf.addToRestrictList("columns.comments");
        conf.set("hive.scratch.dir.permission", "777");
        conf.setVar(ConfVars.SCRATCHDIRPERMISSION, "777");

        File baseDirFile = new File(baseDir);
        baseDirFile.mkdirs();
        baseDirFile.setReadable(true, false);
        baseDirFile.setWritable(true, false);
        baseDirFile.setExecutable(true, false);

        /*HashSet<PosixFilePermission> permissions = Sets.newHashSet();
        //add owners permission
        permissions.add(PosixFilePermission.OWNER_READ);
        //add group permissions
        permissions.add(PosixFilePermission.GROUP_READ);
        permissions.add(PosixFilePermission.GROUP_WRITE);
        permissions.add(PosixFilePermission.GROUP_EXECUTE);
        //add others permissions
        permissions.add(PosixFilePermission.OTHERS_READ);
        permissions.add(PosixFilePermission.OTHERS_EXECUTE);
        Files.setPosixFilePermissions(baseDirFile.toPath(), permissions);*/

        int random = new Random().nextInt();
        conf.set("hive.metastore.warehouse.dir", baseDir + "/warehouse" + random);
        conf.set("hive.metastore.metadb.dir", baseDir + "/metastore_db" + random);
        conf.set("hive.exec.scratchdir", baseDir);
        conf.set("fs.permissions.umask-mode", "022");
        conf.set("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=" + baseDir + "/metastore_db" + random + ";create=true");
        conf.set("hive.metastore.local", "true");
        conf.set("hive.aux.jars.path", "");
        conf.set("hive.added.jars.path", "");
        conf.set("hive.added.files.path", "");
        conf.set("hive.added.archives.path", "");
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        // clear mapred.job.tracker - Hadoop defaults to 'local' if not defined.
        // Hive however expects this to be set to 'local' - if it's not, it does a remote execution (i.e. no child JVM)
        Field field = Configuration.class.getDeclaredField("properties");
        field.setAccessible(true);
        Properties props = (Properties) field.get(conf);
        props.remove("mapred.job.tracker");
        props.remove("mapreduce.framework.name");
        props.setProperty("fs.default.name", "file:///"); // required to not use HDFS and use local file system instead

        // intercept SessionState to clean the thread local
        Field tss = SessionState.class.getDeclaredField("tss");
        tss.setAccessible(true);
        return new HiveConf(conf);
    }

    public void stop() {
        if (hiveServer != null) {
            LOGGER.info("Stopping Hive Local/Embedded Server...");
            hiveServer.stop();
            hiveServer = null;
            config = null;
            LOGGER.info("Hive Local/Embedded Server Stopped Successfully...");
            started = false;
        }
    }
}
