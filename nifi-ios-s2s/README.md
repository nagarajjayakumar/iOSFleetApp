# Apache NiFi Site-to-Site iOS Cocoa Framework 

A lightweight, easy-to-use, Cocoa Framework for sending data to NiFi via the Site-to-Site protocol implemented in Objective-C with primarily Apple-provided Objective-C/C library dependencies. Synchronous and asynchronous interface methods are provided via a low-level site-to-site client and a higher-level site-to-site service that wraps the client. This Cocoa framework will run on iOS devices and simulators.

For the most part, this implementation uses dependencies provided by the Apple platform. The one exception to this is the third-party FMDB, a lightweight SQLite interface, which is used internally by s2s as a mechanism for persistent queuing of flow file data packets when the asynchronous interface is invoked.

## Structure and XCode Schemes

* s2s: iOS Cocoa Framework
* s2sTests: Tests for the s2s iOS Cocoa Framework
* Demo: An Objective-C iOS App that demonstrates basic usage of the s2s iOS Cocoa Framework (NiFiSiteToSiteClient)
* DemoSwift: A Swift iOS App that demonstrates slightly more advanced usage of the s2s iOS Cocoa Framework (NiFiSiteToSiteService)

## Development Environment Requirements

* A Mac (tested on MacOS 10.12.5)
* XCode (tested with XCode 8)
* [The latest version of Carthage](https://github.com/Carthage/Carthage/releases), a dependency manager used for pulling in FMDB, a third-party framework used by s2s
* An iOS device or simulator (tested on iOS 9 and later, running on iPhone SE simulator device)

## Building

The included XCode Project (nifisitetosite.xcodeproj) can be used for building using the XCode IDE or XCode command-line tools.

Here are the commands for building and running the test suite from the command line:

```shell
carthage bootstrap
xcodebuild test -scheme s2sTests -destination 'platform=iOS Simulator,name=iPhone 7'
```

The first command will run Carthage in the project directory. It uses the top-level Cartfile as its input and will download and build FMDB.
The second command builds s2s and s2sTests, which are run in the specified destination, in this case an iPhone 7 iOS Simulator device.

The included XCode project case also be opened in the XCode IDE, as its own standalone project or added to a workspace containing another project (e.g., the app for which you want to use the s2s framework).

## Usage

### From Objective-C

Here is a basic usage example of the s2s Cocoa Framework from Objective-C.

```objective-c
NiFiSiteToSiteRemoteClusterConfig *remoteNiFiInstance =
    [NiFiSiteToSiteRemoteClusterConfig configWithUrl:[NSURL URLWithString:@"http://localhost:8080"]];
NiFiSiteToSiteClientConfig *s2sConfig = [NiFiSiteToSiteClientConfig configWithRemoteCluster: remoteNiFiInstance];
s2sConfig.portName = @"From iOS";

id s2sClient = [NiFiSiteToSiteClient clientWithConfig:s2sConfig];

id transaction = [s2sClient createTransaction];

NSDictionary * attributes1 = @{@"packetNumber": @"1"};
NSData * data1 = [@"Data Packet 1" dataUsingEncoding:NSUTF8StringEncoding];
id dataPacket1 = [NiFiDataPacket dataPacketWithAttributes:attributes1 data:data1];
[transaction sendData:dataPacket1];

NSDictionary * attributes2 = @{@"packetNumber": @"2"};
NSData * data2 = [@"Data Packet 2" dataUsingEncoding:NSUTF8StringEncoding];
id dataPacket2 = [NiFiDataPacket dataPacketWithAttributes:attributes2 data:data2];
[transaction sendData:dataPacket2];

NiFiTransactionResult *transactionResult = [transaction confirmAndCompleteOrError:nil];
```

Note: For Objective-C apps, note that s2s assumes Automatic Reference Counting (ARC) and is not recomended for use in applications compiled in Manual Memory Management mode.

### From Swift

As an Objective-C Cocoa Framework, s2s also defines a Swift module and can be imported as such into a Swift or mixed-language project. The s2s API is nullability-hinted to assist Swift developers.

To use the s2s module in your Swift code, add the s2s.framework to your target's Linked Frameworks and Libraries, and then add this module import to the top of your .swift source code file:

```
import s2s
```

For more background information on mixing Swift and Objective-C, see Apple's 
[Developer Guide for mixing Objective-C and Swift](https://developer.apple.com/library/content/documentation/Swift/Conceptual/BuildingCocoaApps/MixandMatch.html). In this case, we are following the steps for [Importing External Frameworks](https://developer.apple.com/library/content/documentation/Swift/Conceptual/BuildingCocoaApps/MixandMatch.html#//apple_ref/doc/uid/TP40014216-CH10-ID134).

Below are basic usage examples of the s2s Cocoa Framework as a module in Swift.

### SiteToSiteClient

#### Send Synchronously

```swift
let s2sClientConfig = NiFiSiteToSiteClientConfig()
s2sClientConfig.addRemoteCluster(NiFiSiteToSiteRemoteClusterConfig(url: URL(string: "http://localhost:8080")))
s2sConfig.portName = "From iOS";

let s2sClient = NiFiSiteToSiteClient(config: s2sClientConfig)

let transaction = s2sClient.createTransaction()

let data1 = NiFiDataPacket(attributes: ["packetNumber": "1"],
                           data: "Data Packet 1".data(using: String.Encoding.utf8))
transaction?.sendData(data1)

let data2 = NiFiDataPacket(attributes: ["packetNumber": "2"],
                           data: "Data Packet 2".data(using: String.Encoding.utf8))
transaction?.sendData(data2)

do {
    let transactionResult = try transaction?.confirmAndCompleteOrError()
    print("Sent", transactionResult!.dataPacketsTransferred,  "packets!")
} catch {
    print(error.localizedDescription)
}
```

### SiteToSiteService

#### Send asynchronously
```swift
let s2sClientConfig = NiFiSiteToSiteClientConfig()
s2sClientConfig.addRemoteCluster(NiFiSiteToSiteRemoteClusterConfig(url: URL(string: "http://localhost:8080")))
s2sConfig.portName = "From iOS";

let data1 = NiFiDataPacket(attributes: ["packetNumber": "1"],
                           data: "Data Packet 1".data(using: String.Encoding.utf8))

let data2 = NiFiDataPacket(attributes: ["packetNumber": "2"],
                           data: "Data Packet 2".data(using: String.Encoding.utf8))

let packetsToSend = [data1, data2];

NiFiSiteToSiteService.sendDataPackets(packetsToSend, config: s2sClientConfig) { (transactionResult, error) in
    print("Sent", transactionResult!.dataPacketsTransferred,  "packets!")
}
```

##### Add to local persistent queue be sent at some point in the future, with retry, age-off, etc.

```swift
import s2s

// ...

// Early in the app set your s2s client configuration

let s2sClientConfig = NiFiQueuedSiteToSiteClientConfig()
s2sClientConfig.addRemoteCluster(NiFiSiteToSiteRemoteClusterConfig(url: URL(string: "http://localhost:8080")))
s2sConfig.portName = "From iOS";
s2sClientConfig.dataPacketPrioritizer = NiFiNoOpDataPacketPrioritizer(fixedTTL: 60.0)

// ...

// Best Practice: Setup some form of periodic queue processing and cleaning events

func queuedOperationCompleted(status: NiFiSiteToSiteQueueStatus?, error: Error?) {
    // Process completion event

    // The status parameter holds queue count, size, and an "isFull" flag.
}

func cleanAndProcessQueue() {
    NSLog("Cleaning and processing NiFi SiteToSite queue")

    // Cleanup deletes queued packets based on maxQueueCount or maxQueueSize or if packets are older than their TTL
    NiFiSiteToSiteService.cleanupQueuedPackets(with: self.s2sClientConfig,
                                               completionHandler: self.queuedOperationCompleted)
        
    // Process sends the next batch (based on configured batchCount or batchSize) to the remote NiFi
    NiFiSiteToSiteService.processQueuedPackets(with: self.s2sClientConfig,
                                               completionHandler: self.queuedOperationCompleted)
    }

queueProcessingTimer = Timer.scheduledTimer(timeInterval: queueProcessingInterval,
                                            target: self,
                                            selector: #selector(cleanAndProcessQueue),
                                            userInfo: nil,
                                            repeats: true)

// ... 

// At any point in the app, when we have data to send.
let dataPacket = NiFiDataPacket(attributes: ["key1": "value1"],
                                data: "This is the content of the data packet".data(using: String.Encoding.utf8))
NiFiSiteToSiteService.enqueueDataPacket(dataPacket, 
                                        config: s2sClientConfig, 
                                        completionHandler: queuedOperationCompleted)
```

For a more complete example, see the included DemoSwift application.

## Demo Apps and Framework Test Plan

The functionality of this framework is verified by two methods:
* Automated testing via XCode unit tests in the s2sTests target
* Manual testing via included demo apps

In addition to verifying functionality, these serve as good examples of 
how to use the framework API.

To run the tests, select 's2sTests' as the active scheme in XCode, switch to the Test Navigator in the left panel, and click the play icon next to a test or test suite to run the tests.

To run one of the demo apps, select 'DemoSwift' or 'Demo' as the active scheme in XCode and click the Build and Play scheme button.

## Network Connection Configuration

The s2s framework uses Apple's [NSURL family of APIs](https://developer.apple.com/documentation/foundation/nsurl) internally, 
i.e. NSURLSession. You can customize this in `NiFiSiteToSiteRemoteClusterConfig` by specifying a custom 
[NSURLSessionConfiguration](https://developer.apple.com/documentation/foundation/nsurlsessionconfiguration) and/or 
[NSURLSessionDelegate](https://developer.apple.com/documentation/foundation/nsurlsessiondelegate).

This gives you a lot of flexibility and control regarding how connections are established to each remote NiFi cluster. Two
specific types of connection configuration, [secure connections](#security) and [proxy settings](#proxy-configuration), 
are covered in this document.

### Security

#### Server Identity

The S2S Framework can use TLS when communicating to a NiFI server, provided the NiFi server is configured for secure communication.

If a NiFi server is using a certificate signed by a [trusted root Certificate Authority](https://support.apple.com/en-us/HT204132), 
all that is required is to configure the site-to-site client with a `https://` URL for the remote NiFi cluster. HTTPS will be used as the  transport protocol.

If the NiFi server is using a self-signed certificate (e.g., a test environment), or your app needs to perform nonstandard TLS chain 
validation for some other reason, there is more you must do to make the system trust the CA. It is recommended you add that authority's 
trust anchor as described at the bottom of the [Apple Secure Networking Guide](https://developer.apple.com/library/content/documentation/NetworkingInternetWeb/Conceptual/NetworkingOverview/SecureNetworking/SecureNetworking.html).
Alternatively, although not a recommended practice, you can override TLS chain validation with custom logic. The s2s framework uses Apple's 
[NSURL family of APIs](https://developer.apple.com/documentation/foundation/nsurl) internally, i.e. NSURLSession. Therefore, in order 
to provide custom TLS chain validation your app must implement a URLSessionDelegate that overrides the 
function [URLSession:didReceiveChallenge:completionHandler:](https://developer.apple.com/documentation/foundation/nsurlsessiondelegate/1409308-urlsession). Within 
your authentication handler delegate method, you should check to see if the challenge protection space has an authentication type of NSURLAuthenticationMethodServerTrust,
 and if so, obtain the serverTrust information from that protection space. 

An example of this is provided in DemoSwift's [AppNetworking.swift](DemoSwift/AppNetworking.swift) file.

For more information, see "Performing Custom TLS Chain Validation" in [Apple's URL Session Programming Guide](https://developer.apple.com/library/content/documentation/Cocoa/Conceptual/URLLoadingSystem/Articles/AuthenticationChallenges.html) for more information. You might also find these resources informative:

* [Apple Technical Note TN2232: HTTPS Server Trust Evaluation](https://developer.apple.com/library/content/technotes/tn2232/_index.html)
* [iOS Networking Topics > Overriding TLS Chain Validation Correctly](https://developer.apple.com/library/content/documentation/NetworkingInternet/Conceptual/NetworkingTopics/Articles/OverridingSSLChainValidationCorrectly.html)
* [Apple Secure Networking Guide](https://developer.apple.com/library/content/documentation/NetworkingInternetWeb/Conceptual/NetworkingOverview/SecureNetworking/SecureNetworking.html)

#### Client Identity

Client authentication to the NiFi server is supported in two ways:

* Username and password credentials, which can be specified in the site-to-site client configuration.
* Client certificate for two-way TLS.

For client certificate authentication, you must provide a client certificate and implement a URLSessionDelegate that responds to the server's TLS handshake challenge by providing a client certification as 
described in [Apple's URL Session Programming Guide](https://developer.apple.com/library/content/documentation/Cocoa/Conceptual/URLLoadingSystem/Articles/AuthenticationChallenges.html).

Note that due to a limitation of Apple's iOS platform, client certificate authentication is not compatible with background network tasks, 
as they are performed by a system process, and Apple's platform currently does not allow passing client credentials between processes.
Therefore, if your app requires to make use of the s2s module in the background, it is recommended you use username/password client credentials, which
are utilized in the library to communicate with the server to create an authentication token for site-to-site communication. For more information on
this limitation of Apple's iOS platform, see: https://forums.developer.apple.com/thread/28713

#### Socket Protocol Security

When using the `TCP_SOCKET` transport protocol for SiteToSite (instead of the default `HTTP`), there is an additional step for securing the transport layer with an SSL Context.

Internally, this transport protocol is built using the CFStream API. You can pass through the TLS/SSL settings to use for the connection in the NiFiSiteToSiteRemoteClusterConfig.socketTLSSettings field. The settings are a NSDictionary, with key/values well documented in Apple's developer documentation for CFStreams:

* [CFStrem Property SSL Settings](https://developer.apple.com/documentation/cfnetwork/kcfstreampropertysslsettings)
* [CFStream Property SSL Settings Constants](https://developer.apple.com/documentation/corefoundation/cfstream/cfstream_property_ssl_settings_constants)

If you do not set a value for the `socketTLSSettings` config field, the connection will be unsecure. If you want a secure connection using the iOS platform's default SSL settings (reasonable in most cases, assuming you are using a server certificate signed by a root, third-party CA) then pass an empty dictionary.

### Proxy Configuration

NiFi clusters cans be configured to be accessed via an HTTP/S proxy for the SiteToSite protocol. If this is how the remote NiFi cluster is configured, and you wish to
connect via a proxy, this is possible via the `SiteToSiteConfig.urlSessionConfiguration` field.

For information on how to do this, see the following resources:

* [NSURLSessionConfiguration](https://developer.apple.com/documentation/foundation/nsurlsessionconfiguration)
* [NSURLSessionConfiguration.connectionProxyDictionary](https://developer.apple.com/documentation/foundation/nsurlsessionconfiguration)
   * [Global Proxy Settings](https://developer.apple.com/documentation/cfnetwork/global_proxy_settings_constants)
   * [CFNetwork Property Keys](https://developer.apple.com/documentation/cfnetwork/property_keys)

Here is an Objective-C example:

```objective-c
NSMutableDictionary *proxyConfigDictionary = [NSMutableDictionary dictionary];
proxyConfigDictionary[(NSString *)kCFProxyTypeHTTPS] = @(1);
proxyConfigDictionary[(NSString *)kCFProxyHostNameKey] = @"myproxy.example.com";
proxyConfigDictionary[(NSString *)kCFProxyPortNumberKey] = @(8080);
proxyConfigDictionary[(NSString *)kCFProxyUsernameKey] = @"proxyUsername";
proxyConfigDictionary[(NSString *)kCFProxyPasswordKey] = @"proxyPassword";

NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig = ...
remoteClusterConfig.urlSessionConfiguration = [NSURLSessionConfiguration defaultSessionConfiguration];
// Set other URL Session Configuration options as desired ...
remoteClusterConfig.urlSessionConfiguration.connectionProxyDictionary = proxyConfigDictionary;
```

## FAQ and Troubleshooting

*Q: In my application logs I see, "Unable to discover port id for site-to-site input port with name '...'. Server returned status code '403'."*

A: This is a permissions issue with the user you are using to connect to the NiFi API. Check the Policies menu in the NiFI UI and make sure the user has an access policy for 'retrieve site-to-site details'.


*Q: I cannot successfully send data over a secure connection. In my application logs I see, "ERROR  An SSL error has occurred and a secure connection to the server cannot be made."*

A: TLS validation is failing, e.g., it could be that TLS chain validation of the server's certificate is failing. See the Security section above for how to configure your app to communicate over TLS.


*Q: I cannot successfully send data over a secure connection. In my application logs I see, "kCFStreamErrorDomainSSL"*

A: You are implementing custom TLS chain validation, but have not configured your app to allow that. You must add "Allow Arbitrary Loads":YES to "App Transport Security Settings" dict in Info.plist. See Demo or DemoSwift's [Info.plist](DemoSwift/Info.plist) file for an example.

