package org.apache.nifi.security.credstore;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.Key;
import java.security.KeyStore;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HadoopCredentialStoreTest {

    private static final Path TEST_DIR_PATH = Paths.get("target", "credstoretest");
    private static final char[] DEFAULT_PASSWORD = "none".toCharArray();

    @BeforeClass
    public static void createTestDir() throws Exception {
        Files.createDirectories(TEST_DIR_PATH);
    }

    @AfterClass
    public static void removeTestDir() throws Exception {
        FileUtils.deleteDirectory(TEST_DIR_PATH.toFile());
    }

    @Test
    public void testWithAbsoluteCredStorePath() throws Exception {
        String credStoreName = "test01.jceks";
        String credStorePath = String.format("%s/%s", TEST_DIR_PATH.toAbsolutePath(), credStoreName);
        String alias = "alias";
        String password = "password";

        new HadoopCredentialStore(credStorePath)
                .addCredential(alias, password)
                .save();

        assertCredStore(credStoreName, alias, password);
    }

    @Test
    public void testWithRelativeCredStorePath() throws Exception {
        String credStoreName = "test02.jceks";
        String credStorePath = String.format("%s/%s", TEST_DIR_PATH, credStoreName);
        String alias = "alias";
        String password = "password";

        new HadoopCredentialStore(credStorePath)
                .addCredential(alias, password)
                .save();

        assertCredStore(credStoreName, alias, password);
    }

    @Test
    public void testWithCredStoreUri() throws Exception {
        String credStoreName = "test03.jceks";
        String credStoreUri = String.format("jceks://file%s/%s", TEST_DIR_PATH.toAbsolutePath(), credStoreName);
        String alias = "alias";
        String password = "password";

        new HadoopCredentialStore(credStoreUri)
                .addCredential(alias, password)
                .save();

        assertCredStore(credStoreName, alias, password);
    }

    @Test
    public void testWithMultipleCredentials() throws Exception {
        String credStoreName = "test04.jceks";
        String credStorePath = String.format("%s/%s", TEST_DIR_PATH, credStoreName);
        String alias1 = "alias1";
        String password1 = "password1";
        String alias2 = "alias2";
        String password2 = "password2";
        String alias3 = "alias3";
        String password3 = "password3";

        new HadoopCredentialStore(credStorePath)
                .addCredential(alias1, password1)
                .addCredential(alias2, password2)
                .addCredential(alias3, password3)
                .save();

        assertCredStore(credStoreName, alias1, password1, alias2, password2, alias3, password3);
    }

    @Test
    public void testWithNoCredentials() throws Exception {
        String credStoreName = "test05.jceks";
        String credStorePath = String.format("%s/%s", TEST_DIR_PATH, credStoreName);

        new HadoopCredentialStore(credStorePath)
                .save();

        assertCredStore(credStoreName);
    }

    @Test
    public void testWithCredStorePasswordFromEnvVar() throws Exception {
        String credStoreName = "test06.jceks";
        String credStorePath = String.format("%s/%s", TEST_DIR_PATH, credStoreName);
        String credStorePassword = "credStorePassword";
        String alias = "alias";
        String password = "password";

        HadoopCredentialStore credStore = new HadoopCredentialStore(credStorePath) {
            @Override
            Map<String, String> getSystemEnv() {
                return Collections.singletonMap("HADOOP_CREDSTORE_PASSWORD", credStorePassword);
            }
        };

        credStore.addCredential(alias, password);
        credStore.save();

        assertCredStore(credStoreName, credStorePassword.toCharArray(), alias, password);
    }

    private void assertCredStore(String credStoreName, String... aliasPasswordPairs) throws Exception {
        assertCredStore(credStoreName, DEFAULT_PASSWORD, aliasPasswordPairs);
    }

    private void assertCredStore(String credStoreName, char[] credStorePassword, String... aliasPasswordPairs) throws Exception {
        KeyStore credStore = KeyStore.getInstance("JCEKS");
        credStore.load(new FileInputStream(TEST_DIR_PATH.toString() + "/" + credStoreName), credStorePassword);

        int i = 0;
        while (i < aliasPasswordPairs.length) {
            String alias = aliasPasswordPairs[i++];
            String password = aliasPasswordPairs[i++];

            Key key = credStore.getKey(alias, credStorePassword);
            assertEquals(password, new String(key.getEncoded()));
        }
    }

}
