# Azure Replicated Table Library

__RTable__ ("Replicated Table") is a library that provides synchronous geo-replication 
capability over the Azure Table service. The library is self-contained and has no service 
dependencies, making it an excellent tool for disaster recovery and business continuity 
scenarios on Azure.

## RTable Summary (refer to the docs folder for details)

RTable provides synchronous geo-replication for Azure tables, thereby enabling 
zero-RPO availability and disaster recovery in the event of Azure table failures. Clients 
continue to benefit from the amazing scale, lower cost and manageability of Azure 
storage while gaining disaster tolerance. The protocol is optimized for read-latency – 
which is the same as for a regular Azure table read. In addition, recovery actions, such as 
re-introducing a replica, do not impact read or write availability. The protocol runs 
entirely from client-side and does not require any external service. Customers control 
the number and location of regions for replication. For more detailed documentaton, refer to the docs folder.

Latest: See a talk on RTable at Facebook's Networking@Scale

https://www.youtube.com/watch?v=ktXYnSvHbZ4&feature=share

### Pros

- Synchronous geo-replication over Azure tables to enable zero RPO availability and 
  disaster recovery.
- Very minimal change needed to an existing Azure table client. Many existing tools, 
  such as, storage explorer, continue to work without any change.
- Read latency same as without replication.
- No external service is required.

### Cons

- Increased write latency. The latency depends on the locations chosen.
- Write latency increases linearly with the number of replicas. Since the underlying 
  Azure tables are highly durable, the main design goal of the protocol was to provide 
  availability in the event of temporary Azure table outages. It is best suited for 
  replication across two or three regions.
- The protocol runs on the client side. So any changes to the protocol will require 
  clients to update the library.

### Technical Attributes of note

The RTable protocol provides strong consistency and as such is best suited for applications that need zero RPO recovery. The protocol favors Consistency and Partition Tolerance in the CAP theorem. Table operations may experience failure in the event of an Azure table failure until the faulty replica is taken out of rotation. Our goal is to limit this unavailability period to be less than five minutes.

## Getting Started with RTable

### Building source code

To get started, clone the repo and build using `msbuild` or Visual Studio.

The RTable library has the following build dependencies (NuGet packages):

- Azure storage library: `WindowsAzure.Storage.8.7.0` (and its own dependencies)
- JSON.NET: `Newtonsoft.Json.10.0.3`
- Configuration: `Microsoft.WindowsAzure.ConfigurationManager.1.8.0.0`
- NUnit Framework
- FiddlerCore

The dependent packages will be automatically downloaded using NuGet.

### Tests
A comprehensive suite of tests can be found under the Test folder. To run unit tests:

1. Build the entire solution including Tests.csproj
2. Go to folder: test\bin
3. Update the RTableTestConfiguration.xml to update the storage account names and keys (replace the ******). You need three storage accounts.
  - The storage accounts must be configured with `Secure transfer required` set to `Disabled` for tests using the HttpMangler to succeed.
4. (Optional) To test token auth, also update the `InteractiveLoginTenantId` with the subscription's tenant that houses the storage accounts mentioned above.
  - All storage accounts must have role assignments for the logged-in user.
    - Storage Blob Data Contributor (ba92f5b4-2d11-453d-a403-e96b0029c9fe)
    - Storage Table Data Contributor (0a9a7e1f-b9d0-4cc4-a60d-0319b160aaa3)
5. Download nunit.Runners package
6. Run `nunit-console.exe test\bin\Microsoft.Azure.Toolkit.Replication.Test.dll to run all tests
  - If nuget dependencies have already been installed, this exe can be found under `packages\NUnit.ConsoleRunner.3.6.1\tools\nunit3-console.exe`.

### NuGet

At this time, the RTable library is only being released as a source download. To use 
the component, a developer will need to build it themselves.

In the future, as the library matures, a great experience, including published 
NuGet packages, will appear. Thanks for your patience.

## Compared with Azure Table Geo-replication

The Microsoft Azure Storage Service supports creating accounts with geo-replication 
enabled. The replication provided by the service works with a paired region and is 
an excellent choice for replication.

However, the replication does not happen as quickly. When zero RPO is preferred, this 
library can help make that happen.

# RTable History

A component initially built for use by the Azure Network software engineering team 
at Microsoft, `rtable` is a replication library that provides strong consistency and 
is easy to use, built on top of the existing Azure Table experience you may be 
familiar with from the Azure Storage SDK.

# Contributing to RTable

Please feel free to open bugs and issues on GitHub.

For information about contributing to Azure open source projects, see the general 
guidance, including the legal CLA requirement, here: http://azure.github.io/guidelines.html#cla

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

# License

azure-rtable ver. 0.9

Copyright (c) Microsoft Corporation

All rights reserved. 

MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the ""Software""), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

