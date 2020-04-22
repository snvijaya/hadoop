<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Hadoop Azure Support: ABFS  — Azure Data Lake Storage Gen2

<!-- MACRO{toc|fromDepth=1|toDepth=3} -->

## Introduction

The `hadoop-azure` module provides support for the Azure Data Lake Storage Gen2
storage layer through the "abfs" connector

To make it part of Apache Hadoop's default classpath, simply make sure that
`HADOOP_OPTIONAL_TOOLS` in `hadoop-env.sh` has `hadoop-azure` in the list.

## Features

* Read and write data stored in an Azure Blob Storage account.
* *Fully Consistent* view of the storage across all clients.
* Can read data written through the wasb: connector.
* Present a hierarchical file system view by implementing the standard Hadoop
  [`FileSystem`](../api/org/apache/hadoop/fs/FileSystem.html) interface.
* Supports configuration of multiple Azure Blob Storage accounts.
* Can act as a source or destination of data in Hadoop MapReduce, Apache Hive, Apache Spark
* Tested at scale on both Linux and Windows.
* Can be used as a replacement for HDFS on Hadoop clusters deployed in Azure infrastructure.



## <a name="authentication"></a> Authentication

Authentication for ABFS is ultimately granted by [Azure Active Directory](https://docs.microsoft.com/en-us/azure/active-directory/develop/authentication-scenarios).

The concepts covered there are beyond the scope of this document to cover;
developers are expected to have read and understood the concepts therein
to take advantage of the different authentication mechanisms.

What is covered here, briefly, is how to configure the ABFS client to authenticate
in different deployment situations.

The ABFS client can be deployed in different ways, with its authentication needs
driven by them.

1. With the storage account's authentication secret in the configuration:
"Shared Key".
1. Using OAuth 2.0 tokens of one form or another.
1. Deployed in-Azure with the Azure VMs providing OAuth 2.0 tokens to the application,
 "Managed Instance".

What can be changed is what secrets/credentials are used to authenticate the caller.

The authentication mechanism is set in `fs.azure.account.auth.type` (or the account specific variant),
and, for the various OAuth options `fs.azure.account.oauth.provider.type`

All secrets can be stored in JCEKS files. These are encrypted and password
protected —use them or a compatible Hadoop Key Management Store wherever
possible

### <a name="shared-key-auth"></a> Default: Shared Key

This is the simplest authentication mechanism of account + password.

The account name is inferred from the URL;
the password, "key", retrieved from the XML/JCECKs configuration files.

```xml
<property>
  <name>fs.azure.account.auth.type.abfswales1.dfs.core.windows.net</name>
  <value>SharedKey</value>
  <description>
  </description>
</property>
<property>
  <name>fs.azure.account.key.abfswales1.dfs.core.windows.net</name>
  <value>ZGlkIHlvdSByZWFsbHkgdGhpbmsgSSB3YXMgZ29pbmcgdG8gcHV0IGEga2V5IGluIGhlcmU/IA==</value>
  <description>
  The secret password. Never share these.
  </description>
</property>
```

*Note*: The source of the account key can be changed through a custom key provider;
one exists to execute a shell script to retrieve it.

### <a name="oauth-client-credentials"></a> OAuth 2.0 Client Credentials

OAuth 2.0 credentials of (client id, client secret, endpoint) are provided in the configuration/JCEKS file.

The specifics of this process is covered
in [hadoop-azure-datalake](../hadoop-azure-datalake/index.html#Configuring_Credentials_and_FileSystem);
the key names are slightly different here.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider</value>
  <description>
  Use client credentials
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.endpoint</name>
  <value></value>
  <description>
  URL of OAuth endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value></value>
  <description>
  Client ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.secret</name>
  <value></value>
  <description>
  Secret
  </description>
</property>
```

### <a name="oauth-user-and-passwd"></a> OAuth 2.0: Username and Password

An OAuth 2.0 endpoint, username and password are provided in the configuration/JCEKS file.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.UserPasswordTokenProvider</value>
  <description>
  Use user and password
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.endpoint</name>
  <value></value>
  <description>
  URL of OAuth 2.0 endpoint
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.user.name</name>
  <value></value>
  <description>
  username
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.user.password</name>
  <value></value>
  <description>
  password for account
  </description>
</property>
```

### <a name="oauth-refresh-token"></a> OAuth 2.0: Refresh Token

With an existing Oauth 2.0 token, make a request of the Active Directory endpoint
`https://login.microsoftonline.com/Common/oauth2/token` for this token to be refreshed.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth 2.0 authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.RefreshTokenBasedTokenProvider</value>
  <description>
  Use the Refresh Token Provider
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.refresh.token</name>
  <value></value>
  <description>
  Refresh token
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value></value>
  <description>
  Optional Client ID
  </description>
</property>
```

### <a name="managed-identity"></a> Azure Managed Identity

[Azure Managed Identities](https://docs.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/overview), formerly "Managed Service Identities".

OAuth 2.0 tokens are issued by a special endpoint only accessible
from the executing VM (`http://169.254.169.254/metadata/identity/oauth2/token`).
The issued credentials can be used to authenticate.

The Azure Portal/CLI is used to create the service identity.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>OAuth</value>
  <description>
  Use OAuth authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value>org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider</value>
  <description>
  Use MSI for issuing OAuth tokens
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.msi.tenant</name>
  <value></value>
  <description>
  Optional MSI Tenant ID
  </description>
</property>
<property>
  <name>fs.azure.account.oauth2.client.id</name>
  <value></value>
  <description>
  Optional Client ID
  </description>
</property>
```

### Custom OAuth 2.0 Token Provider

A Custom OAuth 2.0 token provider supplies the ABFS connector with an OAuth 2.0
token when its `getAccessToken()` method is invoked.

```xml
<property>
  <name>fs.azure.account.auth.type</name>
  <value>Custom</value>
  <description>
  Custom Authentication
  </description>
</property>
<property>
  <name>fs.azure.account.oauth.provider.type</name>
  <value></value>
  <description>
  classname of Custom Authentication Provider
  </description>
</property>
```

The declared class must implement `org.apache.hadoop.fs.azurebfs.extensions.CustomTokenProviderAdaptee`
and optionally `org.apache.hadoop.fs.azurebfs.extensions.BoundDTExtension`.

The declared class also holds responsibility to implement retry logic while fetching access tokens.

## <a name="technical"></a> Technical notes

### <a name="proxy"></a> Proxy setup

The connector uses the JVM proxy settings to control its proxy setup.

See The [Oracle Java documentation](https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html) for the options to set.

As the connector uses HTTPS by default, the `https.proxyHost` and `https.proxyPort`
options are those which must be configured.

In MapReduce jobs, including distcp, the proxy options must be set in both the
`mapreduce.map.java.opts` and `mapreduce.reduce.java.opts`.

```bash
# this variable is only here to avoid typing the same values twice.
# It's name is not important.
export DISTCP_PROXY_OPTS="-Dhttps.proxyHost=web-proxy.example.com -Dhttps.proxyPort=80"

hadoop distcp \
  -D mapreduce.map.java.opts="$DISTCP_PROXY_OPTS" \
  -D mapreduce.reduce.java.opts="$DISTCP_PROXY_OPTS" \
  -update -skipcrccheck -numListstatusThreads 40 \
  hdfs://namenode:8020/users/alice abfs://backups@account.dfs.core.windows.net/users/alice
```

Without these settings, even though access to ADLS may work from the command line,
`distcp` access can fail with network errors.

### <a name="security"></a> Security

As with other object stores, login secrets are valuable pieces of information.
Organizations should have a process for safely sharing them.

### <a name="limitations"></a> Limitations of the ABFS connector

* File last access time is not tracked.


## Technical notes

### Security

### Consistency and Concurrency

*TODO*: complete/review

The abfs client has a fully consistent view of the store, which has complete Create Read Update and Delete consistency for data and metadata.
(Compare and contrast with S3 which only offers Create consistency; S3Guard adds CRUD to metadata, but not the underlying data).

### Performance

*TODO*: check these.

* File Rename: `O(1)`.
* Directory Rename: `O(files)`.
* Directory Delete: `O(files)`.

## Configuring ABFS

Any configuration can be specified generally (or as the default when accessing all accounts) or can be tied to s a specific account.
For example, an OAuth identity can be configured for use regardless of which account is accessed with the property
"fs.azure.account.oauth2.client.id"
or you can configure an identity to be used only for a specific storage account with
"fs.azure.account.oauth2.client.id.\<account\_name\>.dfs.core.windows.net".

Note that it doesn't make sense to do this with some properties, like shared keys that are inherently account-specific.

### <a name="flushconfigoptions"></a> Flush Options

#### <a name="abfsflushconfigoptions"></a> 1. Azure Blob File System Flush Options
Config `fs.azure.enable.flush` provides an option to render ABFS flush APIs -
 HFlush() and HSync() to be no-op. By default, this
config will be set to true.

Both the APIs will ensure that data is persisted.

#### <a name="outputstreamflushconfigoptions"></a> 2. OutputStream Flush Options
Config `fs.azure.disable.outputstream.flush` provides an option to render
OutputStream Flush() API to be a no-op in AbfsOutputStream. By default, this
config will be set to true.

Hflush() being the only documented API that can provide persistent data
transfer, Flush() also attempting to persist buffered data will lead to
performance issues.

## <a name="Featureconfigoptions"></a> 
### <a name="ioconfigoptions"></a> IO Options
The following configs are related to read and write operations.

`fs.azure.io.retry.max.retries`: Sets the number of retries for IO operations.
Currently this is used only for the server call retry logic. Used within
AbfsClient class as part of the ExponentialRetryPolicy. The value should be
greater than or equal to 0.

`fs.azure.write.request.size`: To set the write buffer size. Specify the value
in bytes. The value should be between 16384 to 104857600 both inclusive (16 KB
to 100 MB). The default value will be 8388608 (8 MB).

`fs.azure.read.request.size`: To set the read buffer size.Specify the value in
bytes. The value should be between 16384 to 104857600 both inclusive (16 KB to
100 MB). The default value will be 4194304 (4 MB).

`fs.azure.read.alwaysReadBufferSize`: Read request size configured by
`fs.azure.read.request.size` will be honoured only when the reads done are in
sequential pattern. When the read pattern is detected to be random, read size
will be same as the buffer length provided by the calling process.
This config when set to true will force random reads to also read in same
request sizes as sequential reads. This is a means to have same read patterns
as of ADLS Gen1, as it does not differentiate read patterns and always reads by
the configured read request size. The default value for this config will be
false, where reads for the provided buffer length is done when random read
pattern is detected.

`fs.azure.readaheadqueue.depth`: Sets the readahead queue depth in
AbfsInputStream. In case the set value is negative the read ahead queue depth
will be set as Runtime.getRuntime().availableProcessors(). By default the value
will be -1. To disable readaheads, set this value to 0. If your workload is
 doing only random reads (non-sequential) or you are seeing throttling, you
  may try setting this value to 0.

`fs.azure.read.readahead.blocksize`: To set the read buffer size for the read
aheads. Specify the value in bytes. The value should be between 16384 to
104857600 both inclusive (16 KB to 100 MB). The default value will be
4194304 (4 MB).

### <a name="securityconfigoptions"></a> Security Options
`fs.azure.always.use.https`: Enforces to use HTTPS instead of HTTP when the flag
is made true. Irrespective of the flag, AbfsClient will use HTTPS if the secure
scheme (ABFSS) is used or OAuth is used for authentication. By default this will
be set to true.

`fs.azure.ssl.channel.mode`: Initializing DelegatingSSLSocketFactory with the
specified SSL channel mode. Value should be of the enum
DelegatingSSLSocketFactory.SSLChannelMode. The default value will be
DelegatingSSLSocketFactory.SSLChannelMode.Default.

### <a name="throttlingconfigoptions"></a> Throttling Options
ABFS driver has the capability to throttle read and write operations to achieve
maximum throughput by minimizing errors. The errors occur when the account
ingress or egress limits are exceeded and, the server-side throttles requests.
Server-side throttling causes the retry policy to be used, but the retry policy
sleeps for long periods of time causing the total ingress or egress throughput
to be as much as 35% lower than optimal. The retry policy is also after the
fact, in that it applies after a request fails. On the other hand, the
client-side throttling implemented here happens before requests are made and
sleeps just enough to minimize errors, allowing optimal ingress and/or egress
throughput. By default the throttling mechanism is enabled in the driver. The
same can be disabled by setting the config `fs.azure.enable.autothrottling`
to false.

### <a name="renameconfigoptions"></a> Rename Options
`fs.azure.atomic.rename.key`: Directories for atomic rename support can be
specified comma separated in this config. The driver prints the following
warning log if the source of the rename belongs to one of the configured
directories. "The atomic rename feature is not supported by the ABFS scheme
; however, rename, create and delete operations are atomic if Namespace is
enabled for your Azure Storage account."
The directories can be specified as comma separated values. By default the value
is "/hbase"

### <a name="perfoptions"></a> Perf Options

#### <a name="abfstracklatencyoptions"></a> 1. HTTP Request Tracking Options
If you set `fs.azure.abfs.latency.track` to `true`, the module starts tracking the
performance metrics of ABFS HTTP traffic. To obtain these numbers on your machine
or cluster, you will also need to enable debug logging for the `AbfsPerfTracker`
class in your `log4j` config. A typical perf log line appears like:

```
h=KARMA t=2019-10-25T20:21:14.518Z a=abfstest01.dfs.core.windows.net
c=abfs-testcontainer-84828169-6488-4a62-a875-1e674275a29f cr=delete ce=deletePath
r=Succeeded l=32 ls=32 lc=1 s=200 e= ci=95121dae-70a8-4187-b067-614091034558
ri=97effdcf-201f-0097-2d71-8bae00000000 ct=0 st=0 rt=0 bs=0 br=0 m=DELETE
u=https%3A%2F%2Fabfstest01.dfs.core.windows.net%2Ftestcontainer%2Ftest%3Ftimeout%3D90%26recursive%3Dtrue
```

The fields have the following definitions:

`h`: host name
`t`: time when this request was logged
`a`: Azure storage account name
`c`: container name
`cr`: name of the caller method
`ce`: name of the callee method
`r`: result (Succeeded/Failed)
`l`: latency (time spent in callee)
`ls`: latency sum (aggregate time spent in caller; logged when there are multiple
callees; logged with the last callee)
`lc`: latency count (number of callees; logged when there are multiple callees;
logged with the last callee)
`s`: HTTP Status code
`e`: Error code
`ci`: client request ID
`ri`: server request ID
`ct`: connection time in milliseconds
`st`: sending time in milliseconds
`rt`: receiving time in milliseconds
`bs`: bytes sent
`br`: bytes received
`m`: HTTP method (GET, PUT etc)
`u`: Encoded HTTP URL

Note that these performance numbers are also sent back to the ADLS Gen 2 API endpoints
in the `x-ms-abfs-client-latency` HTTP headers in subsequent requests. Azure uses these
settings to track their end-to-end latency.

## <a name="troubleshooting"></a> Troubleshooting

The problems associated with the connector usually come down to, in order

1. Classpath.
1. Network setup (proxy etc.).
1. Authentication and Authorization.
1. Anything else.

If you log `org.apache.hadoop.fs.azurebfs.services` at `DEBUG` then you will
see more details about any request which is failing.

One useful tool for debugging connectivity is the [cloudstore storediag utility](https://github.com/steveloughran/cloudstore/releases).

This validates the classpath, the settings, then tries to work with the filesystem.

```bash
bin/hadoop jar cloudstore-0.1-SNAPSHOT.jar storediag abfs://container@account.dfs.core.windows.net/
```

1. If the `storediag` command cannot work with an abfs store, nothing else is likely to.
1. If the `storediag` store does successfully work, that does not guarantee that the classpath
or configuration on the rest of the cluster is also going to work, especially
in distributed applications. But it is at least a start.

### `ClassNotFoundException: org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem`

The `hadoop-azure` JAR is not on the classpah.

```
java.lang.RuntimeException: java.lang.ClassNotFoundException:
    Class org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem not found
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2625)
  at org.apache.hadoop.fs.FileSystem.getFileSystemClass(FileSystem.java:3290)
  at org.apache.hadoop.fs.FileSystem.createFileSystem(FileSystem.java:3322)
  at org.apache.hadoop.fs.FileSystem.access$200(FileSystem.java:136)
  at org.apache.hadoop.fs.FileSystem$Cache.getInternal(FileSystem.java:3373)
  at org.apache.hadoop.fs.FileSystem$Cache.get(FileSystem.java:3341)
  at org.apache.hadoop.fs.FileSystem.get(FileSystem.java:491)
  at org.apache.hadoop.fs.Path.getFileSystem(Path.java:361)
Caused by: java.lang.ClassNotFoundException:
    Class org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem not found
  at org.apache.hadoop.conf.Configuration.getClassByName(Configuration.java:2529)
  at org.apache.hadoop.conf.Configuration.getClass(Configuration.java:2623)
  ... 16 more
```

Tip: if this is happening on the command line, you can turn on debug logging
of the hadoop scripts:

```bash
export HADOOP_SHELL_SCRIPT_DEBUG=true
```

If this is happening on an application running within the cluster, it means
the cluster (somehow) needs to be configured so that the `hadoop-azure`
module and dependencies are on the classpath of deployed applications.

### `ClassNotFoundException: com.microsoft.azure.storage.StorageErrorCode`

### <a name="flushconfigoptions"></a> Access Options
Config `fs.azure.enable.check.access` needs to be set true to enable
 the AzureBlobFileSystem.access().

See the relevant section in [Testing Azure](testing_azure.html).

## References

* [A closer look at Azure Data Lake Storage Gen2](https://azure.microsoft.com/en-gb/blog/a-closer-look-at-azure-data-lake-storage-gen2/);
MSDN Article from June 28, 2018.
