/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.standard;


import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestListFile  {

    final String TESTDIR = "target/test/data/in";
    final File testDir = new File(TESTDIR);
    ListFile processor;
    TestRunner runner;
    ProcessContext context;

    // Testing factors in milliseconds for file ages that are configured on each run by resetAges()
    // age#millis are relative time references
    // time#millis are absolute time references
    // age#filter are filter label strings for the filter properties
    Long syncTime = System.currentTimeMillis();
    Long time0millis, time1millis, time2millis, time3millis, time4millis, time5millis;
    Long age0millis, age1millis, age2millis, age3millis, age4millis, age5millis;
    String age0, age1, age2, age3, age4, age5;

    @Before
    public void setUp() throws Exception {
        processor = new ListFile();
        runner = TestRunners.newTestRunner(processor);
        context = runner.getProcessContext();
        deleteDirectory(testDir);
        assertTrue("Unable to create test data directory " + testDir.getAbsolutePath(), testDir.exists() || testDir.mkdirs());
        resetAges();
    }

    @After
    public void tearDown() throws Exception {
        deleteDirectory(testDir);
        File tempFile = processor.getPersistenceFile();
        if (tempFile.exists()) {
            assertTrue(tempFile.delete());
        }
//        assertTrue(processor.getPersistenceFile().delete());
    }

    @Test
    public void testAttributes() throws IOException {
        final File file1 = new File(TESTDIR + "/attrib1.txt");
        assertTrue(file1.createNewFile());
        FileOutputStream fos = new FileOutputStream(file1);
        fos.write(new byte[1234]);
        fos.close();
        assertTrue(file1.setLastModified(time1millis));
        Long time1rounded = time1millis - time1millis % 1000;

        final PosixFileAttributes attrib1 = Files.readAttributes(file1.toPath(), PosixFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        assertEquals(System.getProperty("user.name"), attrib1.owner().getName());
        assertEquals(System.getProperty("user.name"), attrib1.group().getName());
        assertEquals(time1rounded.longValue(), attrib1.lastModifiedTime().toMillis());
        assertEquals(1234, attrib1.size());
        assertEquals("rw-rw-r--", perms2string(attrib1.permissions().toString()));
    }

    @Test
    public void testGetSupportedPropertyDescriptors() throws Exception {
        List<PropertyDescriptor> properties = processor.getSupportedPropertyDescriptors();
        assertEquals(9, properties.size());
        assertEquals(ListFile.DIRECTORY, properties.get(0));
        assertEquals(ListFile.RECURSE, properties.get(1));
        assertEquals(ListFile.FILE_FILTER, properties.get(2));
        assertEquals(ListFile.PATH_FILTER, properties.get(3));
        assertEquals(ListFile.MIN_AGE, properties.get(4));
        assertEquals(ListFile.MAX_AGE, properties.get(5));
        assertEquals(ListFile.MIN_SIZE, properties.get(6));
        assertEquals(ListFile.MAX_SIZE, properties.get(7));
        assertEquals(ListFile.IGNORE_HIDDEN_FILES, properties.get(8));
    }

    @Test
    public void testGetRelationships() throws Exception {
        Set<Relationship> relationships = processor.getRelationships();
        assertEquals(1, relationships.size());
        assertEquals(AbstractListProcessor.REL_SUCCESS, relationships.toArray()[0]);
    }

    @Test
    public void testGetPath() {
        runner.setProperty(ListFile.DIRECTORY, "/dir/test1");
        assertEquals("/dir/test1", processor.getPath(context));
        runner.setProperty(ListFile.DIRECTORY, "${literal(\"/DIR/TEST2\"):toLower()}");
        assertEquals("/dir/test2", processor.getPath(context));
    }

    @Test
    public void testPerformListing() throws Exception {
        // create first file
        final File file1 = new File(TESTDIR + "/listing1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(time4millis));

        // process first file and set new timestamp
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles.size());

        // create second file
        final File file2 = new File(TESTDIR + "/listing2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(time2millis));

        // process second file after timestamp
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());

        // create third file
        final File file3 = new File(TESTDIR + "/listing3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(time4millis));

        // process third file before timestamp
        runner.clearTransferState();
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(0, successFiles3.size());

        // force state to reset and process all files
        runner.clearTransferState();
        runner.removeProperty(ListFile.DIRECTORY);
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles4.size());
    }

    @Test
    public void testFilterAge() throws IOException {
        final File file1 = new File(TESTDIR + "/age1.txt");
        assertTrue(file1.createNewFile());
        assertTrue(file1.setLastModified(time0millis));

        final File file2 = new File(TESTDIR + "/age2.txt");
        assertTrue(file2.createNewFile());
        assertTrue(file2.setLastModified(time2millis));

        final File file3 = new File(TESTDIR + "/age3.txt");
        assertTrue(file3.createNewFile());
        assertTrue(file3.setLastModified(time4millis));

        dumpTestDir(testDir);
        System.out.println("testFilterAge syncTime=         " + syncTime);
        System.out.println("testFilterAge now=              " + System.currentTimeMillis());
        System.out.println("testFilterAge age0=             " + age0);
        System.out.println("testFilterAge age1=             " + age1);
        System.out.println("testFilterAge age2=             " + age2);
        System.out.println("testFilterAge age3=             " + age3);
        System.out.println("testFilterAge age4=             " + age4);
        System.out.println("testFilterAge age5=             " + age5);

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles1.size());

        // exclude oldest
        runner.clearTransferState();
        runner.setProperty(ListFile.MIN_AGE, age0);
        runner.setProperty(ListFile.MAX_AGE, age3);
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());

        // exclude newest
        runner.clearTransferState();
        runner.setProperty(ListFile.MIN_AGE, age1);
        runner.setProperty(ListFile.MAX_AGE, age5);
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles3.size());

        // exclude oldest and newest
        runner.clearTransferState();
        runner.setProperty(ListFile.MIN_AGE, age1);
        runner.setProperty(ListFile.MAX_AGE, age3);
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles4.size());
    }

    @Test
    public void testFilterSize() throws IOException {
        final byte[] bytes1000 = new byte[1000];
        final byte[] bytes5000 = new byte[5000];
        final byte[] bytes10000 = new byte[10000];
        FileOutputStream fos;

        final File file1 = new File(TESTDIR + "/size1.txt");
        assertTrue(file1.createNewFile());
        fos = new FileOutputStream(file1);
        fos.write(bytes10000);
        fos.close();

        final File file2 = new File(TESTDIR + "/size2.txt");
        assertTrue(file2.createNewFile());
        fos = new FileOutputStream(file2);
        fos.write(bytes5000);
        fos.close();

        final File file3 = new File(TESTDIR + "/size3.txt");
        assertTrue(file3.createNewFile());
        fos = new FileOutputStream(file3);
        fos.write(bytes1000);
        fos.close();

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles1.size());

        // exclude largest
        runner.clearTransferState();
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "0 b");
        runner.setProperty(ListFile.MAX_SIZE, "7500 b");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());

        // exclude smallest
        runner.clearTransferState();
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "2500 b");
        runner.removeProperty(ListFile.MAX_SIZE);
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles3.size());

        // exclude oldest and newest
        runner.clearTransferState();
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.setProperty(ListFile.MIN_SIZE, "2500 b");
        runner.setProperty(ListFile.MAX_SIZE, "7500 b");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles4 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles4.size());
    }

    @Test
    public void testFilterHidden() throws IOException {
        FileOutputStream fos;

        final File file1 = new File(TESTDIR + "/hidden1.txt");
        assertTrue(file1.createNewFile());
        fos = new FileOutputStream(file1);
        fos.close();

        final File file2 = new File(TESTDIR + "/.hidden2.txt");
        assertTrue(file2.createNewFile());
        fos = new FileOutputStream(file2);
        fos.close();

        // check all files
        runner.clearTransferState();
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ".*");
        runner.removeProperty(ListFile.MIN_AGE);
        runner.removeProperty(ListFile.MAX_AGE);
        runner.removeProperty(ListFile.MIN_SIZE);
        runner.removeProperty(ListFile.MAX_SIZE);
        runner.setProperty(ListFile.IGNORE_HIDDEN_FILES, "false");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles1.size());

        // exclude hidden
        runner.clearTransferState();
        runner.setProperty(ListFile.IGNORE_HIDDEN_FILES, "true");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());
    }

    @Test
    public void testFilterFilePattern() throws IOException {
        final File file1 = new File(TESTDIR + "/file1-abc-apple.txt");
        assertTrue(file1.createNewFile());

        final File file2 = new File(TESTDIR + "/file2-xyz-apple.txt");
        assertTrue(file2.createNewFile());

        final File file3 = new File(TESTDIR + "/file3-xyz-banana.txt");
        assertTrue(file3.createNewFile());

        final File file4 = new File(TESTDIR + "/file4-pdq-banana.txt");
        assertTrue(file4.createNewFile());

        // check all files
        runner.clearTransferState();
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ListFile.FILE_FILTER.getDefaultValue());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(4, successFiles1.size());

        // filter file on pattern
        runner.clearTransferState();
        runner.setProperty(ListFile.FILE_FILTER, ".*-xyz-.*");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());
    }

    @Test
    public void testFilterPathPattern() throws IOException {
        final File subdir1 = new File(TESTDIR + "/subdir1");
        assertTrue(subdir1.mkdirs());

        final File subdir2 = new File(TESTDIR + "/subdir1/subdir2");
        assertTrue(subdir2.mkdirs());

        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());

        final File file2 = new File(TESTDIR + "/subdir1/file2.txt");
        assertTrue(file2.createNewFile());

        final File file3 = new File(TESTDIR + "/subdir1/subdir2/file3.txt");
        assertTrue(file3.createNewFile());

        final File file4 = new File(TESTDIR + "/subdir1/file4.txt");
        assertTrue(file4.createNewFile());

        // check all files
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.FILE_FILTER, ListFile.FILE_FILTER.getDefaultValue());
        runner.setProperty(ListFile.RECURSE, "true");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(4, successFiles1.size());
        for (MockFlowFile file : successFiles1) {
            System.out.println("successFiles1=" + file.getAttribute("filename"));
        }
        System.out.flush();

        // filter path on pattern subdir1
        runner.clearTransferState();
        runner.setProperty(ListFile.PATH_FILTER, "subdir1");
        runner.setProperty(ListFile.RECURSE, "true");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles2.size());
        for (MockFlowFile file : successFiles2) {
            System.out.println("successFiles2=" + file.getAttribute("filename"));
        }
        System.out.flush();

        // filter path on pattern subdir2
        runner.clearTransferState();
        runner.setProperty(ListFile.PATH_FILTER, "subdir2");
        runner.setProperty(ListFile.RECURSE, "true");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles3 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles3.size());
        for (MockFlowFile file : successFiles3) {
            System.out.println("successFiles3=" + file.getAttribute("filename"));
        }
        System.out.flush();
    }

    @Test
    public void testRecurse() throws IOException {
        final File subdir1 = new File(TESTDIR + "/subdir1");
        assertTrue(subdir1.mkdirs());

        final File subdir2 = new File(TESTDIR + "/subdir1/subdir2");
        assertTrue(subdir2.mkdirs());

        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());

        final File file2 = new File(TESTDIR + "/subdir1/file2.txt");
        assertTrue(file2.createNewFile());

        final File file3 = new File(TESTDIR + "/subdir1/subdir2/file3.txt");
        assertTrue(file3.createNewFile());

        // check all files
        runner.clearTransferState();
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.RECURSE, "true");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles1.size());

        // exclude hidden
        runner.clearTransferState();
        runner.setProperty(ListFile.RECURSE, "false");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles2.size());
    }

    @Test
    public void testReadable() throws IOException {
        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());

        final File file2 = new File(TESTDIR + "/file2.txt");
        assertTrue(file2.createNewFile());

        final File file3 = new File(TESTDIR + "/file3.txt");
        assertTrue(file3.createNewFile());

        // check all files
        runner.clearTransferState();
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.setProperty(ListFile.RECURSE, "true");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(3, successFiles1.size());

        // make file2 unreadable
        assertTrue(file2.setReadable(false));
        runner.clearTransferState();
        runner.setProperty(ListFile.RECURSE, "false");
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles2 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(2, successFiles2.size());
    }

    @Test
    public void testAttributesSet() throws IOException {
        final File file1 = new File(TESTDIR + "/file1.txt");
        assertTrue(file1.createNewFile());
        FileOutputStream fos = new FileOutputStream(file1);
        fos.write(new byte[1234]);
        fos.close();
        assertTrue(file1.setLastModified(time3millis));
        String time3rounded = Long.toString(time3millis - time3millis % 1000);

        // check all files
        runner.clearTransferState();
        runner.setProperty(ListFile.DIRECTORY, testDir.getAbsolutePath());
        runner.run();
        runner.assertAllFlowFilesTransferred(ListFile.REL_SUCCESS);
        final List<MockFlowFile> successFiles1 = runner.getFlowFilesForRelationship(ListFile.REL_SUCCESS);
        assertEquals(1, successFiles1.size());
        MockFlowFile mock1 = successFiles1.get(0);
        assertEquals("file1.txt", mock1.getAttribute("filename"));
        assertEquals(testDir.getAbsolutePath(), mock1.getAttribute("path"));
        assertEquals(System.getProperty("user.name"), mock1.getAttribute("fs.owner"));
        assertEquals(System.getProperty("user.name"), mock1.getAttribute("fs.group"));
        assertEquals(time3rounded, mock1.getAttribute("fs.lastModified"));
        assertEquals("1234", mock1.getAttribute("fs.length"));
        assertEquals("rw-rw-r--", perms2string(mock1.getAttribute("fs.permissions")));
    }

    @Test
    public void testIsListingResetNecessary() throws Exception {
        assertEquals(true, processor.isListingResetNecessary(ListFile.DIRECTORY));
        assertEquals(true, processor.isListingResetNecessary(ListFile.RECURSE));
        assertEquals(true, processor.isListingResetNecessary(ListFile.FILE_FILTER));
        assertEquals(true, processor.isListingResetNecessary(ListFile.PATH_FILTER));
        assertEquals(true, processor.isListingResetNecessary(ListFile.MIN_AGE));
        assertEquals(true, processor.isListingResetNecessary(ListFile.MAX_AGE));
        assertEquals(true, processor.isListingResetNecessary(ListFile.MIN_SIZE));
        assertEquals(true, processor.isListingResetNecessary(ListFile.MAX_SIZE));
        assertEquals(true, processor.isListingResetNecessary(ListFile.IGNORE_HIDDEN_FILES));
        assertEquals(false, processor.isListingResetNecessary(new PropertyDescriptor.Builder().name("x").build()));
    }

    public void resetAges() {
        syncTime = System.currentTimeMillis();

        age0millis = 0L;
        age1millis = 2000L;
        age2millis = 5000L;
        age3millis = 7000L;
        age4millis = 10000L;
        age5millis = 100000L;

        time0millis = syncTime - age0millis;
        time1millis = syncTime - age1millis;
        time2millis = syncTime - age2millis;
        time3millis = syncTime - age3millis;
        time4millis = syncTime - age4millis;
        time5millis = syncTime - age5millis;

        age0 = Long.toString(age0millis) + " millis";
        age1 = Long.toString(age1millis) + " millis";
        age2 = Long.toString(age2millis) + " millis";
        age3 = Long.toString(age3millis) + " millis";
        age4 = Long.toString(age4millis) + " millis";
        age5 = Long.toString(age5millis) + " millis";
    }

    private void deleteDirectory(final File directory) throws IOException {
        if (directory.exists()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (final File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    }
                    assertTrue("Could not delete " + file.getAbsolutePath(), file.delete());
                }
            }
        }
    }

    private String perms2string(final String permissions) {
        final StringBuilder sb = new StringBuilder();
        if (permissions.contains(PosixFilePermission.OWNER_READ.toString())) {
            sb.append("r");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.OWNER_WRITE.toString())) {
            sb.append("w");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.OWNER_EXECUTE.toString())) {
            sb.append("x");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.GROUP_READ.toString())) {
            sb.append("r");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.GROUP_WRITE.toString())) {
            sb.append("w");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.GROUP_EXECUTE.toString())) {
            sb.append("x");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.OTHERS_READ.toString())) {
            sb.append("r");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.OTHERS_WRITE.toString())) {
            sb.append("w");
        } else {
            sb.append("-");
        }
        if (permissions.contains(PosixFilePermission.OTHERS_EXECUTE.toString())) {
            sb.append("x");
        } else {
            sb.append("-");
        }
        return sb.toString();
    }

    /*
     * Because of difficulty tracking some of these tests, age especially, this can be used during test development to
     * show physical file listings for troubleshooting file creation and test execution.
     */
    public void dumpTestDir(final File top) throws IOException {
        final String format = "%1s %9s %19s %13d %s\n";
        final File target = (top == null) ? testDir : top;
        final DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final File[] dirFiles = target.listFiles();
        if (dirFiles != null) {
            for (File f : dirFiles) {
                System.out.printf(format,
                        (f.isDirectory() ? 'd' : 'f'),
                        perms2string(Files.getPosixFilePermissions(f.toPath(), LinkOption.NOFOLLOW_LINKS).toString()),
                        formatter.format(f.lastModified()),
                        f.lastModified(),
                        f.getPath());
                if (f.isDirectory()) {
                    dumpTestDir(target);
                }
            }
        }
    }
}
