mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAbfsInputStreamStatistics						 > fullRun/ITestAbfsInputStreamStatistics
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAbfsStatistics                                    > fullRun/ITestAbfsStatistics
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAbfsStreamStatistics                              > fullRun/ITestAbfsStreamStatistics
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemCopy                           > fullRun/ITestAzureBlobFileSystemCopy
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemE2E                            > fullRun/ITestAzureBlobFileSystemE2E
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemFlush                          > fullRun/ITestAzureBlobFileSystemFlush
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemMainOperation                  > fullRun/ITestAzureBlobFileSystemMainOperation
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemOauth                          > fullRun/ITestAzureBlobFileSystemOauth
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemRandomRead                     > fullRun/ITestAzureBlobFileSystemRandomRead
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestFileSystemProperties                              > fullRun/ITestFileSystemProperties
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.TestAbfsInputStream                           > fullRun/TestAbfsInputStream
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAbfsReadWriteAndSeek                              > fullRun/ITestAbfsReadWriteAndSeek
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAzureBlobFileSystemE2EScale                       > fullRun/ITestAzureBlobFileSystemE2EScale
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestSmallWriteOptimization                            > fullRun/ITestSmallWriteOptimization
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.ITestAbfsInputStream                          > fullRun/ITestAbfsInputStream
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.ITestAbfsInputStreamReadFooter                > fullRun/ITestAbfsInputStreamReadFooter
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.ITestAbfsInputStreamSmallFileReads            > fullRun/ITestAbfsInputStreamSmallFileReads
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.ITestAbfsPositionedRead                       > fullRun/ITestAbfsPositionedRead
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.services.ITestAbfsUnbuffer                             > fullRun/ITestAbfsUnbuffer
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsContractUnbuffer                     > fullRun/ITestAbfsContractUnbuffer
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsFileSystemContractAppend             > fullRun/ITestAbfsFileSystemContractAppend
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsFileSystemContractCreate             > fullRun/ITestAbfsFileSystemContractCreate
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsFileSystemContractMkdir              > fullRun/ITestAbfsFileSystemContractMkdir
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsFileSystemContractOpen               > fullRun/ITestAbfsFileSystemContractOpen
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsFileSystemContractRename             > fullRun/ITestAbfsFileSystemContractRename
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsFileSystemContractSecureDistCp       > fullRun/ITestAbfsFileSystemContractSecureDistCp
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAbfsFileSystemContractSeek               > fullRun/ITestAbfsFileSystemContractSeek
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.contract.ITestAzureBlobFileSystemBasics                > fullRun/ITestAzureBlobFileSystemBasics
mvn test -Dtest=org.apache.hadoop.fs.azurebfs.ITestAbfsNetworkStatistics                             > fullRun/ITestAbfsNetworkStatistics 

cd fullRun/
mkdir errs
./getFailureStatus.sh
 
