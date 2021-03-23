echo "Test path: src/test/java/org/apache/hadoop/fs/azurebfs/services/ITestAbfsFastpath.java"
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.ITestAbfsFastpath 
cat target/surefire-reports/org.apache.hadoop.fs.azurebfs.services.ITestAbfsFastpath-output.txt
