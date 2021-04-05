echo "Test path: /hadoop/src/hadoop/hadoop-tools/hadoop-azure/src/test/java/org/apache/hadoop/fs/azurebfs/services/ITestAbfsFastpath.java"
/usr/lib/apache-maven-3.6.3/bin/mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.ITestAbfsFastpath 
cat target/surefire-reports/org.apache.hadoop.fs.azurebfs.services.ITestAbfsFastpath-output.txt
