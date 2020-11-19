# Commits

**DEVOPS-507: Added changelogs to binary builds**

 * DEVOPS-507 (Add changelog to every plug-in during build) 
 * https://knime-com.atlassian.net/browse/DEVOPS-507 

[d9714454f096a5b](https://bitbucket.org/knime/knime-azure/commits/d9714454f096a5b) Thorsten Meinl *2020-11-19 10:08:56*

**BD-1081: add default blob storage URI exporter**

 * BD-1081 (Implement base set of URIExporters) 

[5189242176c5e7d](https://bitbucket.org/knime/knime-azure/commits/5189242176c5e7d) Sascha Wolke *2020-11-12 16:31:57*

**AP-15534: Add new connector icon**

 * AP-15534 (Use new file system connector node icon for all connectors) 

[36b7c6e74db9267](https://bitbucket.org/knime/knime-azure/commits/36b7c6e74db9267) Bjoern Lohrmann *2020-11-09 10:12:50*

**FHEXT-40: Fix sonar rule violations, use PagedPathIterator**

 * remove duplicate code already in PagedPathIterator. 
 * sonar required a rewrite of fetchAttributesInternal() 
 * FHEXT-40 (Azure Blob Storage Connector) 

[77030a5d7a94008](https://bitbucket.org/knime/knime-azure/commits/77030a5d7a94008) Bjoern Lohrmann *2020-10-14 21:08:26*

**FHEXT-40: Polish node description**

 * FHEXT-40 (Azure Blob Storage Connector) 

[8c326f7484bbaaf](https://bitbucket.org/knime/knime-azure/commits/8c326f7484bbaaf) Bjoern Lohrmann *2020-10-14 13:27:14*

**FHEXT-40: Polish node (error messages, add storage account to FSLocationSpec)**

 * FHEXT-40 (Azure Blob Storage Connector) 

[6ea828501984966](https://bitbucket.org/knime/knime-azure/commits/6ea828501984966) Bjoern Lohrmann *2020-10-14 13:27:14*

**FHEXT-40: Use moveInternal() from BaseFS, add container name validation**

 * FHEXT-40 (Azure Blob Storage Connector) 

[488d7a80de06f98](https://bitbucket.org/knime/knime-azure/commits/488d7a80de06f98) Bjoern Lohrmann *2020-10-14 13:27:14*

**FHEXT-40: Preserver blob type (block/append) when copying a blob**

 * FHEXT-40 (Azure Blob Storage Connector) 

[26d8caca7a4717c](https://bitbucket.org/knime/knime-azure/commits/26d8caca7a4717c) Bjoern Lohrmann *2020-10-14 13:27:14*

**FHEXT-40: Preliminary node icon for Azure BS Connector**

 * FHEXT-40 (Azure Blob Storage Connector) 

[010751d6b450d5a](https://bitbucket.org/knime/knime-azure/commits/010751d6b450d5a) Bjoern Lohrmann *2020-10-14 13:27:14*

**FHEXT-52: OAuth authentication support for Azure Blob Storage**

 * FHEXT-52 (OAuth authentication support for Azure Blob Storage Connector) 

[2d4e8cc429f806b](https://bitbucket.org/knime/knime-azure/commits/2d4e8cc429f806b) Alexander Bondaletov *2020-10-09 15:01:03*

**FHEXT-40: Moved container name validation to createDirectoryInternal()**

 * Previous version caused StackOverflowErrors when browsing the storage account. 
 * FHEXT-40 (Azure Blob Storage Connector) 

[d8b28785ab0ff41](https://bitbucket.org/knime/knime-azure/commits/d8b28785ab0ff41) Bjoern Lohrmann *2020-10-09 14:04:32*

**DEVOPS-483: Update build.properties to include LICENSE file in bin & src jars.**

 * DEVOPS-483 (Add LICENSE.txt files to binary builds of all our plug-ins) 

[9088660ac9bd5fd](https://bitbucket.org/knime/knime-azure/commits/9088660ac9bd5fd) Sebastian Gerau *2020-09-28 22:53:18*

**DEVOPS-483: Add LICENSE.TXT for distribution conformance.**

 * DEVOPS-483 (Add LICENSE.txt files to binary builds of all our plug-ins) 

[06a79cd36bee7d6](https://bitbucket.org/knime/knime-azure/commits/06a79cd36bee7d6) Sebastian Gerau *2020-09-28 19:17:49*

**FHEXT-40 Azure Blob Storage test initializer: directory creation fixed**


[a922fb0c3b4fd26](https://bitbucket.org/knime/knime-azure/commits/a922fb0c3b4fd26) Alexander Bondaletov *2020-09-16 22:55:56*

**FHEXT-40 Azure Blob: container validation for non-normalized paths fixed**


[5ce886030b8923b](https://bitbucket.org/knime/knime-azure/commits/5ce886030b8923b) Alexander Bondaletov *2020-09-11 16:37:49*

**FHEXT-40 Pull request comments addressed.**


[6f189ca37f3a1eb](https://bitbucket.org/knime/knime-azure/commits/6f189ca37f3a1eb) Alexander Bondaletov *2020-09-11 11:32:23*

**FHEXT-40 Azure Blob Storage Connector bugfixing**

 * increased timeout for a download/upload operations 
 * added container name validation 

[671ac92d5794120](https://bitbucket.org/knime/knime-azure/commits/671ac92d5794120) Alexander Bondaletov *2020-09-10 14:51:29*

**FHEXT-35: Fix failing testcase DirectoryStreamTest.test_list_dot_directory()**

 * The working directory for the integration tests is always a random dir 
 * below the workingDirPrefix. 
 * FHEXT-35 (Azure Blob Storage Connector: Add browsability and testability) 

[672328f74cd7ecc](https://bitbucket.org/knime/knime-azure/commits/672328f74cd7ecc) Bjoern Lohrmann *2020-09-07 12:33:19*

**FHEXT-39: Azure Blob Storage Connector: Handle access denied failures**

 * FHEXT-39 (Azure Blob Storage Connector: Handle and test access denied failures) 

[d26df34252a0d0e](https://bitbucket.org/knime/knime-azure/commits/d26df34252a0d0e) Alexander Bondaletov *2020-09-07 12:21:27*

**FHEXT-42: Azure SAS Token authentication**

 * FHEXT-42 (Microsoft Authentication: Add SAS token authentication (for Azure Blob Storage)) 

[6cf65c02a1c0366](https://bitbucket.org/knime/knime-azure/commits/6cf65c02a1c0366) Alexander Bondaletov *2020-09-01 14:41:43*

**FHEXT-33: Azure Blob Storage Connector: local dependencies for Azure Blobstorage**

 * The target platform jars were causing very strange and unexpected behavior. 
 * FHEXT-33 (Azure Blob Storage: Update target platform libraries) 

[51620c7359afc1a](https://bitbucket.org/knime/knime-azure/commits/51620c7359afc1a) Alexander Bondaletov *2020-08-31 07:38:08*

**FHEXT-40: Azure Blob Storage connector: timeout settings refactoring**

 * FHEXT-40 (Azure Blob Storage connector) 

[ebbd57ebf36926f](https://bitbucket.org/knime/knime-azure/commits/ebbd57ebf36926f) Alexander Bondaletov *2020-08-31 07:34:42*

**FHEXT-41 Shared key authentication for Azure Blob Storage**

 * FHEXT-41 (Microsoft Authentication: Add Shared key authentication for Azure Blob Storage) 

[703ef85256992c1](https://bitbucket.org/knime/knime-azure/commits/703ef85256992c1) Alexander Bondaletov *2020-08-27 14:02:18*

**FHEXT-40: Lower file metadata cacheTTL to 6000 millis**

 * FHEXT-40 (Azure Blob Storage Connector) 

[58bb8fdbbfa726c](https://bitbucket.org/knime/knime-azure/commits/58bb8fdbbfa726c) Bjoern Lohrmann *2020-08-17 21:39:08*

**FHEXT-34: Add Sonarlint bindings**

 * FHEXT-34 (Azure Blob Storage Connector: Setup node and file system structure and connection setup) 

[c402d5a1f8f3ec4](https://bitbucket.org/knime/knime-azure/commits/c402d5a1f8f3ec4) Bjoern Lohrmann *2020-08-17 21:13:08*

**FHEXT-40: Azure Blob Storage Connector: node settings and description**

 * FHEXT-40 (Azure Blob Storage Connector) 

[8e667bc7e94a738](https://bitbucket.org/knime/knime-azure/commits/8e667bc7e94a738) Alexander Bondaletov *2020-08-17 20:28:12*

**FHEXT-33: Use target platform libs for Azure BlobStorage and fix classloading**

 * azure-core is loading and HttpClient implementation using the ServiceLoader 
 * framework, hence we need to play some tricks in the plugin activator to ensure 
 * the implementation class is found by the TCCL. 
 * FHEXT-33 (Azure Blob Storage: Update target platform libraries) 

[1b9377b66a409b7](https://bitbucket.org/knime/knime-azure/commits/1b9377b66a409b7) Bjoern Lohrmann *2020-08-17 12:33:18*

**FHEXT-37: Azure Blob Storage Connector: reading and writing**

 * FHEXT-37 (Azure Blob Storage Connector: Implement file reading and writing) 

[e56e12aaad408b3](https://bitbucket.org/knime/knime-azure/commits/e56e12aaad408b3) Alexander Bondaletov *2020-08-11 07:07:46*

**FHEXT-36 Azure blob storage: moveInternal method refactoring**


[7fe855e2be0b285](https://bitbucket.org/knime/knime-azure/commits/7fe855e2be0b285) Alexander Bondaletov *2020-07-27 16:15:12*

**FHEXT-36 Azure Blob Storage Connector: copy/move/delete**


[a36df0b58c86525](https://bitbucket.org/knime/knime-azure/commits/a36df0b58c86525) Alexander Bondaletov *2020-07-22 11:01:26*

**FHEXT-35: Add browsability and testability**

 * FHEXT-35 (Azure Blob Storage Connector: Add browsability and testability) 

[b33e237b7eeb650](https://bitbucket.org/knime/knime-azure/commits/b33e237b7eeb650) Alexander Bondaletov *2020-07-22 10:31:41*

**FHEXT-24: Code skeleton for Azure Blob Storage Connector**

 * FHEXT-24 (Azure Blob Storage Connector: Setup node and file system structure and connection setup) 

[89c9cb4a5cb95ff](https://bitbucket.org/knime/knime-azure/commits/89c9cb4a5cb95ff) Alexander Bondaletov *2020-07-16 08:08:44*

