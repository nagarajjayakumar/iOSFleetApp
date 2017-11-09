/*
 * Copyright 2017 Hortonworks, Inc.
 * All rights reserved.
 *
 *   Hortonworks, Inc. licenses this file to you under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License. You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * See the associated NOTICE file for additional information regarding copyright ownership.
 */

#import <Foundation/Foundation.h>
#import "NiFiSiteToSite.h"
#import "NiFiSiteToSiteConfig.h"
#import "NiFiSiteToSiteClient.h"
#import "NiFiHttpRestApiClient.h"
#import "NiFiSiteToSiteUtil.h"
#import "NiFiSiteToSiteTransaction.h"
#import "NiFiDataPacket.h"
#import "NiFiSocket.h"
#import "NiFiError.h"


#define MSEC_PER_SEC 1000

// MARK: - SiteToSite Internal Interface Extentensions

@interface NiFiSiteToSiteClient()
@property (nonatomic, retain, readwrite, nonnull) NiFiSiteToSiteClientConfig *config;
- (nonnull instancetype) initWithConfig:(nonnull NiFiSiteToSiteClientConfig *)config;
@end


// An abstract base class for clients that want to implement a client for a given protocol to a given cluster
@interface NiFiSiteToSiteUniClusterClient : NiFiSiteToSiteClient
@property (nonatomic, retain, readwrite, nonnull) NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig;
@property (nonatomic, readwrite, nullable)NSArray *prioritizedRemoteInputPortIdList;
@property (atomic, readwrite, nonnull)NSSet *initialPeerKeySet; // key of every peer in initial config
@property (atomic, readwrite, nonnull)NSArray<NiFiPeer *> *currentPeerList;
@property (nonatomic, readwrite) NSTimeInterval nextPeerUpdateTimeIntervalSinceReferenceDate;
@property (nonatomic, readwrite) BOOL isPeerUpdateNecessary;
- (nullable instancetype) initWithConfig:(nonnull NiFiSiteToSiteClientConfig *)config
                          remoteCluster:(nonnull NiFiSiteToSiteRemoteClusterConfig *)remoteClusterConfig;
@end


@interface NiFiHttpSiteToSiteClient : NiFiSiteToSiteUniClusterClient
@end

@interface NiFiSocketSiteToSiteClient : NiFiSiteToSiteUniClusterClient
@end


// MARK: - TransactionResult Implementation

@implementation NiFiTransactionResult

- (nonnull instancetype)init {
    return [self initWithResponseCode:RESERVED dataPacketsTransferred:0 message:nil duration:0];
}

- (nonnull instancetype)initWithResponseCode:(NiFiTransactionResponseCode)responseCode
                      dataPacketsTransferred:(NSUInteger)packetCount
                                     message:(NSString *)message
                                    duration:(NSTimeInterval)duration {
    self = [super init];
    if(self != nil) {
        _responseCode = responseCode;
        _dataPacketsTransferred = packetCount;
        _message = message;
        _duration = duration;
    }
    return self;
}

- (bool)shouldBackoff {
    return _responseCode == TRANSACTION_FINISHED_BUT_DESTINATION_FULL;
}

@end



// MARK: - SiteToSiteMultiClusterClient Implementation

@interface NiFiSiteToSiteMultiClusterClient : NiFiSiteToSiteClient
@property (nonatomic, retain, readwrite, nonnull) NSMutableArray *clusterClients;
- (nullable NSObject <NiFiTransaction> *)createTransactionWithURLSession:(nullable NSURLSession *)urlSession; // redefining nullability
@end


@implementation NiFiSiteToSiteMultiClusterClient

+ (nonnull instancetype) clientWithConfig:(nonnull NiFiSiteToSiteClientConfig *)config {
    if (config && config.remoteClusters && [config.remoteClusters count] > 0) {
        return [[self alloc] initWithConfig:config];
    }
    NSLog(@"No remote clusters configured!");
    return nil;
}

- (instancetype)initWithConfig:(NiFiSiteToSiteClientConfig *)config {
    self = [super initWithConfig:config];
    if (self) {
        [self createClients];
    }
    return self;
}

- (void)createClients {
    _clusterClients = [NSMutableArray arrayWithCapacity:[self.config.remoteClusters count]];
    for (NiFiSiteToSiteRemoteClusterConfig *clusterConfig in self.config.remoteClusters) {
        NiFiSiteToSiteClient *client = nil;
        
        switch (clusterConfig.transportProtocol) {
            case HTTP:
                client = [[NiFiHttpSiteToSiteClient alloc] initWithConfig:self.config remoteCluster:clusterConfig];
                break;
            case TCP_SOCKET:
                client = [[NiFiSocketSiteToSiteClient alloc] initWithConfig:self.config remoteCluster:clusterConfig];
                break;
            default:
                @throw [NSException
                        exceptionWithName:NSGenericException
                        reason:@"Unsupported NiFiSiteToSiteTransportProtocol when creating NiFiSiteToSiteClient."
                        userInfo:nil];
        }
        
        if (client) {
            [_clusterClients addObject:client];
        }
    }
}

- (nullable NSObject <NiFiTransaction> *)createTransaction {
    return [self createTransactionWithURLSession:nil];
}

- (nullable NSObject <NiFiTransaction> *)createTransactionWithURLSession:(NSURLSession *)urlSession {
    for (NiFiSiteToSiteClient *client in _clusterClients) {
        id transaction = urlSession ? [client createTransactionWithURLSession:urlSession] : [client createTransaction];
        if (transaction) {
            return transaction;
        }
    }
    return nil;
}

@end



// MARK: - SiteToSiteClient Implementation

@implementation NiFiTransaction

- (instancetype) initWithPeer:(NiFiPeer *)peer {
    self = [super init];
    if(self != nil) {
        _peer = peer;
        _startTime = [NSDate date];
        _transactionState = TRANSACTION_STARTED;
        _dataPacketEncoder = [[NiFiDataPacketEncoder alloc] init];
    }
    return self;
}

- (nonnull NSString *)transactionId {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

- (NiFiTransactionState)transactionState {
    return _transactionState;
}

- (void)sendData:(nonnull NiFiDataPacket *)data {
    [self.dataPacketEncoder appendDataPacket:data];
    self.transactionState = DATA_EXCHANGED;
}

- (void)cancel {
    self.transactionState = TRANSACTION_CANCELED;
    // subclasses can implement cancel interaction with server
}

- (void)error {
    if (self.peer) {
        [self.peer markFailure];
    }
    self.transactionState = TRANSACTION_ERROR;
}

- (nullable NiFiTransactionResult *)confirmAndCompleteOrError:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

- (nullable NiFiPeer *)getPeer {
    return self.peer;
}

@end


@implementation NiFiSiteToSiteClient

+ (nonnull instancetype) clientWithConfig:(nonnull NiFiSiteToSiteClientConfig *)config {
    return [NiFiSiteToSiteMultiClusterClient clientWithConfig:config];
}

- (nonnull instancetype) initWithConfig:(nonnull NiFiSiteToSiteClientConfig *) config {
    self = [super init];
    if(self != nil) {
        _config = config;
    }
    return self;
}

- (nullable NSObject <NiFiTransaction> *)createTransaction {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

- (nullable NSObject <NiFiTransaction> *)createTransactionWithURLSession:(NSURLSession *)urlSession {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

@end



// MARK: - SiteToSiteUniClusterClient Implementation

@implementation NiFiSiteToSiteUniClusterClient
- (nullable instancetype) initWithConfig:(nonnull NiFiSiteToSiteClientConfig *)config
                          remoteCluster:(nonnull NiFiSiteToSiteRemoteClusterConfig *)remoteClusterConfig {
    self = [super initWithConfig:config];
    if (self) {
        _remoteClusterConfig = remoteClusterConfig;
        [self resetPeersFromInitialPeerConfig];
        if (! _currentPeerList || _currentPeerList.count <= 0) {
            self = nil;
        }
        self.isPeerUpdateNecessary = YES;
        self.nextPeerUpdateTimeIntervalSinceReferenceDate = [NSDate timeIntervalSinceReferenceDate];
    }
    return self;
}

- (nullable NSObject <NiFiTransaction> *)createTransaction {
    return [self createTransactionWithURLSession:[self createUrlSession]];
}

// This is an abstract class. createTransactionWithURLSession:urlSession must be implemented by subclass

- (nullable NiFiPeer *)getPreferredPeer {
    NSArray *sortedPeerList = [self getSortedPeerList];
    if (!sortedPeerList) {
        return nil;
    }
    return sortedPeerList[0];
}

- (NSArray<NiFiPeer *> *)getSortedPeerList {
    if (!_currentPeerList) {
        return nil;
    }
    NSArray *sortedPeerList = [_currentPeerList sortedArrayUsingSelector:@selector(compare:)];
    return sortedPeerList;
}

- (void)resetPeersFromInitialPeerConfig {
    if (_remoteClusterConfig.urls && _remoteClusterConfig.urls.count > 0) {
        _currentPeerList = [NSMutableArray arrayWithCapacity:_remoteClusterConfig.urls.count];
        _initialPeerKeySet = [NSMutableSet setWithCapacity:_remoteClusterConfig.urls.count];
        for (NSURL *url in _remoteClusterConfig.urls) {
            NiFiPeer *peer = [NiFiPeer peerWithUrl:url];
            if (peer) {
                [(NSMutableArray *)_currentPeerList addObject:peer];
                [(NSMutableSet *)_initialPeerKeySet addObject:[peer peerKey]];
            }
        }
    }
}

- (void)updatePeers {
    NSURLSession *urlSession = [self createUrlSession];
    if (! _currentPeerList || _currentPeerList.count < 1) {
        [self resetPeersFromInitialPeerConfig];
    }
    for (NiFiPeer *peer in _currentPeerList) {
        NiFiHttpRestApiClient *apiClient = [self createRestApiClientWithBaseUrl:peer.url
                                                                     urlSession:(NSObject<NSURLSessionProtocol> *)urlSession];
        NSError *getPeersError = nil;
        NSArray *newPeers = [apiClient getPeersOrError:&getPeersError];
        if (getPeersError || !newPeers) {
            NSString *logMsg = @"Failed to update peers for remote NiFi cluster.";
            if (getPeersError) {
                logMsg = [NSString stringWithFormat:@"%@ %@", logMsg, getPeersError.localizedDescription];
            }
            NSLog(@"%@", logMsg);
        } else {
            [self addPeers:newPeers];
            NSLog(@"Successfully updated peers for remote NiFi cluster.");
            self.isPeerUpdateNecessary = NO;
            if (self.config.peerUpdateInterval > 0.0) {
                self.nextPeerUpdateTimeIntervalSinceReferenceDate =
                    [NSDate timeIntervalSinceReferenceDate] + self.config.peerUpdateInterval;
            }
            return;
        }
        
    }
    NSLog(@"Error: Failed to update peers for remote NiFi cluster.");
}

- (void)updatePeersIfNecessary {
    
    if (!self.isPeerUpdateNecessary) {
        // has the configured refresh interval (if set to > 0.0) elapsed?
        self.isPeerUpdateNecessary = (self.config.peerUpdateInterval > 0.0 ?
                                      [NSDate timeIntervalSinceReferenceDate] > self.nextPeerUpdateTimeIntervalSinceReferenceDate :
                                      NO);
    }
    
    if (self.isPeerUpdateNecessary) {
        [self updatePeers];
    }
    
}

- (void)addPeers:(NSArray<NiFiPeer *> *)newPeerList {
    NSMutableDictionary<NSURL *, NiFiPeer *> *newPeerMap = [NSMutableDictionary dictionaryWithCapacity:[newPeerList count]];
    for (NiFiPeer *peer in newPeerList) {
        id newPeerKey = [peer.url absoluteURL];
        [newPeerMap setObject:peer forKey:newPeerKey];
    }
    for (NiFiPeer *peer in _currentPeerList) {
        id oldPeerKey = [peer.url absoluteURL];
        if (newPeerMap[oldPeerKey]) {
            newPeerMap[oldPeerKey].lastFailure = peer.lastFailure;
        } else if ([_initialPeerKeySet containsObject:oldPeerKey]) {
            [newPeerMap setObject:peer forKey:oldPeerKey];
        }
    }
    if (newPeerMap && newPeerMap.count > 0) {
        _currentPeerList = [newPeerMap allValues];
    }
}

// MARK: Helper functions 

- (NSURLSession *)createUrlSession {
    NSURLSession *urlSession;
    if (self.remoteClusterConfig.urlSessionConfiguration ||
            self.remoteClusterConfig.urlSessionDelegate ||
            self.remoteClusterConfig.proxyConfig) {
        
        NSURLSessionConfiguration *configuration = self.remoteClusterConfig.urlSessionConfiguration ?: [NSURLSessionConfiguration defaultSessionConfiguration];
        
        if (self.remoteClusterConfig.proxyConfig) {
            NiFiProxyConfig *proxyConfig = self.remoteClusterConfig.proxyConfig;
            // If the lib caller configured its own proxy settings, use that rather than this attempt to auto-configure
            if (!configuration.connectionProxyDictionary || configuration.connectionProxyDictionary.count == 0) {
                NSMutableDictionary *proxyConfigDictionary = [NSMutableDictionary dictionary];
                if ([proxyConfig.url.scheme isEqualToString:@"http"]) {
                    proxyConfigDictionary[(NSString *)kCFProxyTypeHTTP] = @(1);
                } else if ([proxyConfig.url.scheme isEqualToString:@"https"]) {
                    proxyConfigDictionary[(NSString *)kCFProxyTypeHTTPS] = @(1);
                } else {
                    NSLog(@"Warning: NiFi SiteToSite Proxy URL does not use http or https protocol scheme.");
                }
                
                if (proxyConfig.url && proxyConfig.url.host) {
                    proxyConfigDictionary[(NSString *)kCFProxyHostNameKey] = proxyConfig.url.host;
                }
                if (proxyConfig.url && proxyConfig.url.port) {
                    proxyConfigDictionary[(NSString *)kCFProxyPortNumberKey] = proxyConfig.url.port;
                }
                
                if (proxyConfig.username && proxyConfig.password) {
                    proxyConfigDictionary[(NSString *)kCFProxyUsernameKey] = proxyConfig.username;
                    proxyConfigDictionary[(NSString *)kCFProxyPasswordKey] = proxyConfig.username;
                }
                
                configuration.connectionProxyDictionary = proxyConfigDictionary;
            }
        }
        
        urlSession = [NSURLSession sessionWithConfiguration:configuration
                                                   delegate:self.remoteClusterConfig.urlSessionDelegate
                                              delegateQueue:nil];
    } else {
        urlSession = [NSURLSession sharedSession];
    }
    return urlSession;
}

- (NiFiHttpRestApiClient *)createRestApiClientWithBaseUrl:(NSURL *)url
                                               urlSession:(NSObject<NSURLSessionProtocol> *)urlSession {
    
    /* strip path component of url if one was passed */
    NSURLComponents *urlComponents = [NSURLComponents componentsWithURL:url resolvingAgainstBaseURL:NO];
    if (!urlComponents) {
        NSLog(@"Invalid url '%@' for remote cluster could not be parsed.", url);
    }
    urlComponents.path = nil; // REST API Client constructor expects base url.
    NSURL *apiBaseUrl = urlComponents.URL;
    
    /* create credentials if necessary */
    NSURLCredential *credential = nil;
    if (_remoteClusterConfig.username && _remoteClusterConfig.password) {
        credential = [NSURLCredential credentialWithUser:_remoteClusterConfig.username
                                                password:_remoteClusterConfig.password
                                             persistence:NSURLCredentialPersistenceForSession];
    }
    
    NiFiHttpRestApiClient *restApiClient = [[NiFiHttpRestApiClient alloc] initWithBaseUrl:apiBaseUrl
                                                                         clientCredential:credential
                                                                               urlSession:urlSession];
    
    return restApiClient;
}

- (void) updatePrioritizedPortList:(nonnull NiFiHttpRestApiClient *)restApiClient {

    NSError *portIdLookupError;
    NSDictionary *portIdsByName = [restApiClient getRemoteInputPortsOrError:&portIdLookupError];
    if (portIdLookupError || portIdsByName == nil) {
        NSString *errMsg = portIdLookupError ?
        [NSString stringWithFormat:@"When looking up port ID by name, encountered error with domain=%@, code=%ld, message=%@",
         portIdLookupError.domain,
         (long)portIdLookupError.code,
         portIdLookupError.localizedDescription] :
        @"When looking up port ID by name, encountered error";
        NSLog(@"%@", errMsg);
    }
    
    // The priority of port resolution is currently:
    //   - portID (if provided in the config)
    //   - portID for a given portName
    //   - portID if exactly 1 input port exists at the remote instance / cluster.
    NSMutableArray *prioritizedPortList = [NSMutableArray arrayWithCapacity:1];
    
    if (self.config.portId) {
        [prioritizedPortList addObject:self.config.portId];
    }
    
    if (portIdsByName) {
        if (self.config.portName) {
            NSString *portIdByName = portIdsByName[self.config.portName];
            if (portIdByName && ![prioritizedPortList containsObject:portIdsByName]) {
                [prioritizedPortList addObject:portIdByName];
            }
        }
        
        if ([portIdsByName count] == 1) {
            NSString *solePortId = [portIdsByName allValues][0];
            if (solePortId && ![prioritizedPortList containsObject:solePortId]) {
                [prioritizedPortList addObject:solePortId];
            }
        }
    }
    
    if (prioritizedPortList && [prioritizedPortList count] > 0) {
        _prioritizedRemoteInputPortIdList = prioritizedPortList;
    }
}


@end



// MARK: HttpSiteToSiteClient Implementation

NSString *const HTTP_SITE_TO_SITE_PROTOCOL_VERSION = @"5";


typedef void(^TtlExtenderBlock)(NSString * transactionId);


@implementation NiFiHttpTransaction

- (nonnull instancetype) initWithPortId:(nonnull NSString *)portId
                      httpRestApiClient:(NiFiHttpRestApiClient *)restApiClient {
    return [self initWithPortId:portId httpRestApiClient:restApiClient peer:nil];
}

- (nonnull instancetype) initWithPortId:(nonnull NSString *)portId
                      httpRestApiClient:(nonnull NiFiHttpRestApiClient *)restApiClient
                                   peer:(nullable NiFiPeer *)peer {
    self = [super initWithPeer:peer];
    if (self != nil) {
        _restApiClient = restApiClient;
        NSError *error;
        _transactionResource = [_restApiClient initiateSendTransactionToPortId:portId error:&error];
        if (_transactionResource) {
            self.shouldKeepAlive = true;
            [self scheduleNextKeepAliveWithTTL:(_transactionResource.serverSideTtl)];
        } else {
            NSLog(@"ERROR  %@", [error localizedDescription]);
            [self error];
            self = nil;
        }
    }
    return self;
}

- (NSString *)transactionId {
    return self.transactionResource.transactionId;
}

- (void) sendData:(NiFiDataPacket *)data {
    [super sendData:data]; /* NiFiTransaction */
}

- (void) cancel {
    [super cancel]; /* NiFiTransaction */
    NSError *error;
    self.shouldKeepAlive = false;
    [_restApiClient endTransaction:_transactionResource.transactionUrl responseCode:CANCEL_TRANSACTION error:&error];
}

- (void) error {
    [super error];
    self.shouldKeepAlive = false;
}

- (nullable NiFiTransactionResult *)confirmAndCompleteOrError:(NSError *_Nullable *_Nullable)error {
    
    // 1. Send encoded flow file data
    NSUInteger serverCrc = [self.restApiClient sendFlowFiles:self.dataPacketEncoder
                                             withTransaction:self.transactionResource
                                                       error:error];
    
    NSUInteger expectedCrc = [self.dataPacketEncoder getEncodedDataCrcChecksum];
    
    NSLog(@"NiFi Peer returned CRC code: %ld, expected CRC was: %ld",
          (unsigned long)serverCrc, (unsigned long)expectedCrc);
    
    if (serverCrc != expectedCrc) {
        [self.restApiClient endTransaction:self.transactionResource.transactionUrl
                              responseCode:BAD_CHECKSUM
                                     error:error];
        [self error];
        return nil;
    }
    
    self.transactionState = TRANSACTION_CONFIRMED;
    
    NiFiTransactionResult *transactionResult = [self.restApiClient endTransaction:self.transactionResource.transactionUrl
                                                                     responseCode:CONFIRM_TRANSACTION
                                                                            error:error];
    if ((error && *error) || !transactionResult) {
        [self error];
        return nil;
    }
    self.transactionState = TRANSACTION_COMPLETED;
    transactionResult.duration = [[NSDate date] timeIntervalSinceDate:self.startTime];
    NSLog(@"Completed transaction. flowfiles_sent=%llu, transactionId=%@", transactionResult.dataPacketsTransferred, [self transactionId]);
    self.shouldKeepAlive = false;
    return transactionResult;
}


- (void)scheduleNextKeepAliveWithTTL:(NSTimeInterval)ttl {
    // schedule another keep alive if needed
    if (self.shouldKeepAlive) {
        NSLog(@"Scheduling background task to extend transaction TTL");
        dispatch_time_t nextKeepAlive = dispatch_time(DISPATCH_TIME_NOW, (ttl / 2) * NSEC_PER_SEC);
        dispatch_after(nextKeepAlive, dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0), ^(void){
            if (self &&
                    [self shouldKeepAlive] &&
                    [self restApiClient] &&
                    [self transactionResource] &&
                    [self transactionResource].transactionUrl) {
                NSError *error;
                [_restApiClient extendTTLForTransaction:_transactionResource.transactionUrl error:&error];
                if (error) {
                    NSLog(@"Error extended transaction with id=%@: %@",
                          _transactionResource.transactionId, error.localizedDescription);
                }
                [self scheduleNextKeepAliveWithTTL:ttl]; // this will put the next "keep-alive heartbeat" task on an async queue
            }
        });
    }
}

+ (bool)assertExpectedState:(NiFiTransactionState)expectedState equalsActualState:(NiFiTransactionState)actualState {
    if (expectedState != actualState) {
        NSLog(@"NiFiTransaction encountered internal state error. Expected to be in state %@, actually in state %@",
              [NiFiSiteToSiteUtil NiFiTransactionStateToString:expectedState],
              [NiFiSiteToSiteUtil NiFiTransactionStateToString:actualState]);
        return false;
    }
    return true;
}

@end


@implementation NiFiHttpSiteToSiteClient

- (nullable NSObject <NiFiTransaction> *)createTransactionWithURLSession:(NSURLSession *)urlSession {
    
    [self updatePeersIfNecessary];
    NiFiPeer *peer = [self getPreferredPeer];
    
    NiFiHttpRestApiClient *restApiClient = [self createRestApiClientWithBaseUrl:peer.url
                                                                     urlSession:(NSObject<NSURLSessionProtocol> *)urlSession];
    
    if (!self.prioritizedRemoteInputPortIdList) {
        [self updatePrioritizedPortList:restApiClient];
    }
    
    NiFiHttpTransaction *transaction = nil;
    if (self.prioritizedRemoteInputPortIdList) {
        for (NSString *portId in self.prioritizedRemoteInputPortIdList) {
            NSLog(@"Attempting to initiate transaction. portId=%@", portId);
            transaction = [[NiFiHttpTransaction alloc] initWithPortId:portId httpRestApiClient:restApiClient];
            if (transaction) {
                NSLog(@"Successfully initiated transaction. transactionId=%@, portId=%@",
                      transaction.transactionId, portId);
                break;
            }
        }
    }
    
    if (!transaction) {
        [peer markFailure];
        self.isPeerUpdateNecessary = YES;
        NSLog(@"Could not create NiFi s2s transaction. Check NiFi s2s configuration. "
              "Is the correct url and s2s portName/portId set?");
    }
    return transaction;
}

@end



// MARK: SocketSiteToSiteClient Implementation

static const Byte MAGIC_BYTES[] = {'N', 'i', 'F', 'i'};
static const int MAGIC_BYTES_LEN = 4;

typedef enum {
    TagNone,
    TagMagicBytesWrite,
    TagProtocolHandshakeWrite,
    TagProtocolHandshakeRead,
    TagResponseCodeRead,
} NiFiSocketGCDTags;

@interface NiFiSocketTransaction ()
@property (nonatomic, retain, readwrite, nonnull) NSString *transactionId;
@property (nonatomic, retain, readwrite, nonnull) NiFiSiteToSiteClientConfig *config;
@property (nonatomic, retain, readwrite, nonnull) NiFiSocket *socket;
@property NSInteger protocolVersion;
@property BOOL firstPacketSend;
@end


@implementation NiFiSocketTransaction

- (nonnull instancetype) initWithConfig:(nonnull NiFiSiteToSiteClientConfig *)config
                    remoteClusterConfig:(nonnull NiFiSiteToSiteRemoteClusterConfig *)remoteCluster
                                   peer:(nonnull NiFiPeer *)peer
                                 portId:(nonnull NSString *)portId {
    self = [super initWithPeer:peer];
    if (self) {
        self.firstPacketSend = YES;
        self.transactionId = [[NSUUID UUID] UUIDString];
        self.config = config;
        self.peer = peer;
        uint32_t port = self.peer.rawPort ? [self.peer.rawPort unsignedIntValue] : 0;
        if (!port) {
            NSLog(@"Cannot create socket sitetosite connection without raw port configured for peer.");
            return nil;
        }
        
        NSError *socketError;
        _socket = [NiFiSocket socket];
        NSLog(@"Establishing socket connection. host=%@, port=%i", peer.url.host, port);
        if ([_socket connectToHost:peer.url.host onPort:port error:&socketError]) {
            
            if (remoteCluster.socketTLSSettings) {
                [_socket startTLS:remoteCluster.socketTLSSettings];
            }
            
            [_socket writeData:[NSData dataWithBytes:MAGIC_BYTES length:MAGIC_BYTES_LEN] withTimeout:self.config.timeout callback:nil];
            
            NSInteger clientProtocolVersions[] = {6, 5, 4, 3, 2, 1};
            self.protocolVersion = [self negotiateProtocolVersion:clientProtocolVersions len:6];
            [self protocolHandshake:self.protocolVersion portId:portId];
            
            NSInteger clientCodecVersions[] = {1};
            NSInteger codecVersion = [self negotiateFlowFileCodecVersion:clientCodecVersions len:1];
            if (codecVersion != 1) {
                NSLog(@"NiFi Peer does not support a compatible Flow File Codec Version as this SiteToSite client.");
            }
            
            [_socket writeData:[[self class] javaUTFDataForString:@"SEND_FLOWFILES"] withTimeout:self.config.timeout callback:nil];
        } else {
            NSLog(@"Error with socket s2s configuration.");
            self = nil;
        }
    }
    return self;
}

- (NSInteger) negotiateProtocolVersion:(NSInteger[])prioritizedVersions
                                   len:(NSInteger)prioritizedVersionsLength {
    return [self negotiateVersionForResource:@"SocketFlowFileProtocol"
                         prioritizedVersions:prioritizedVersions
                                         len:prioritizedVersionsLength];
}

- (NSInteger) negotiateFlowFileCodecVersion:(NSInteger[])prioritizedVersions
                                        len:(NSInteger)prioritizedVersionsLength {
    [_socket writeData:[[self class] javaUTFDataForString:@"NEGOTIATE_FLOWFILE_CODEC"] withTimeout:self.config.timeout callback:nil];
    return [self negotiateVersionForResource:@"StandardFlowFileCodec"
                         prioritizedVersions:prioritizedVersions
                                         len:prioritizedVersionsLength];
}

static const int RESOURCE_OK_CODE = 20;
static const int DIFFERENT_RESOURCE_VERSION_CODE = 21;
static const int ABORT_CODE = 255;

- (NSInteger) negotiateVersionForResource:(NSString *)resourceKey
                      prioritizedVersions:(NSInteger[])versions
                                      len:(NSInteger)versionsLength {
    
    NSInteger negotiatedVersion = -1;
    int32_t serverMaxVersion = INT_MAX; // we don't know until we as the server,
                                        // so for now assume the server supports any version of this resource
    
    for (int i=0; i < versionsLength; i++) {
        
        int32_t clientRequestedVersion = (int32_t)versions[i];
        if (clientRequestedVersion < serverMaxVersion) {
            NSLog(@"Negotiating '%@' version with peer. version=%i", resourceKey, clientRequestedVersion);
            
            // we initiate the request by sending the resource key and the version (encoding/protocol/etc) the client wants to use.
            NSMutableData *request = [NSMutableData data];
            [request appendData:[[self class] javaUTFDataForString:resourceKey]];
            uint32_t wireVersion = CFSwapInt32HostToBig((uint32_t)clientRequestedVersion); // host order to network order
            [request appendBytes:&wireVersion length:4];
            
            // ---------- Server Exchange -----------
            NSError *error = nil;
            NSData *responseData = [_socket readDataAfterWriteData:request timeout:self.config.timeout error:&error];
            if (error || !responseData || responseData.length <= 0) {
                if (error) {
                    NSLog(@"Error in %@: %@", NSStringFromSelector(_cmd), error.localizedDescription);
                }
                return -1;
            }
            
            NSUInteger dataLength = responseData.length;
            Byte *responseBytes = (Byte *)[responseData bytes];
            uint8_t serverResponse = responseBytes[0];
            if (serverResponse == RESOURCE_OK_CODE) {
                NSLog(@"Server responded RESOURCE_OK. code=%li", (long)serverResponse);
                negotiatedVersion = clientRequestedVersion;
                break;
            } else if (serverResponse == DIFFERENT_RESOURCE_VERSION_CODE) {
                if (dataLength >= 5) {
                    int32_t buf;
                    memcpy(&buf, &responseBytes[1], 4); // index 1-4 is an int32 in big endian
                    serverMaxVersion = CFSwapInt32BigToHost(buf); // index 1-4 is an int32.
                    NSLog(@"Server responded DIFFERENT_RESOURCE_VERSION. code=%li, max_version=%li", (long)serverResponse, (long)serverMaxVersion);
                }
                else {
                    NSLog(@"Socket Protocol Error. Server responded with DIFFERENT_RESOURCE_VERSION but did not provide a max version. code=%li", (long)serverResponse);
                    return -1;
                }
            } else if (serverResponse == ABORT_CODE) {
                NSLog(@"Server responded with ABORT. code=%li", (long)serverResponse);
                if (dataLength > 1) {
                    NSData *messageData = [responseData subdataWithRange:NSMakeRange(1, dataLength-1)];
                    NSString *message = [[self class] stringForjavaUTFData:messageData];
                    if (message) {
                        NSLog(@"ABORT message='%@'", message);
                    }
                }
                break;
            } else {
                NSLog(@"Server responded with UNKNOWN code. code=%li", (long)serverResponse);
                break;
            }
        }
    }
    
    return negotiatedVersion;
}

- (BOOL) protocolHandshake:(NSInteger)protocolVersion portId:(nonnull NSString *)portId {
    
    if (!portId) {
        NSLog(@"Cannot establish sitetosite protocol connection without remote input portId.");
        return NO;
    }
    
    NSMutableData *request = [NSMutableData data];
    
    NSString *connectionId = [[NSUUID UUID] UUIDString];
    [request appendData:[[self class] javaUTFDataForString:connectionId]];
    
    if (protocolVersion >= 3) {
        NSString *peerURLString = [[[self class] getURLForPeer:self.peer] absoluteString];
        [request appendData:[[self class] javaUTFDataForString:peerURLString]];
    }
    
    NSDictionary *properties = [NSMutableDictionary dictionary];
    [properties setValue:@"false" forKey:@"GZIP"];
    [properties setValue:portId forKey:@"PORT_IDENTIFIER"];
    [properties setValue:[NSString stringWithFormat:@"%li", (long)(MSEC_PER_SEC * self.config.timeout)] forKey:@"REQUEST_EXPIRATION_MILLIS"];
    
    [[self class] appendInt32:(uint32_t)[properties count] toWireData:request];
    for (NSString *propertyKey in [properties allKeys]) {
        [request appendData:[[self class] javaUTFDataForString:propertyKey]];
        [request appendData:[[self class] javaUTFDataForString:properties[propertyKey]]];
    }
    
    // ---------- Server Exchange -----------
    NSError *error;
    NSData *responseData = [self.socket readDataAfterWriteData:request timeout:self.config.timeout error:&error];
    if (error || !responseData || responseData.length <= 0) {
        if (error) {
            NSLog(@"Error in %@: %@", NSStringFromSelector(_cmd), error.localizedDescription);
        }
        return NO;
    }
    
    NiFiTransactionResponseCode responseCode;
    NSString *responseMessage;
    BOOL success = [[self class] parseResponseCodeFromData:responseData responseCode:&responseCode message:&responseMessage];
    if (!success) {
        return NO;
    }
    if (responseCode != PROPERTIES_OK) {
        NSLog(@"Error during sitetotsite protocol handshake. Server responded with response code='%i', message='%@'", responseCode, responseMessage ?: @"");
        return NO;
    }
    
    return YES;
}

- (void) sendData:(NiFiDataPacket *)data {
    if (!self.firstPacketSend) {
        Byte rcBytes[] = {'R', 'C', CONTINUE_TRANSACTION};
        NSData *rcData = [NSData dataWithBytes:rcBytes length:3];
        [self.dataPacketEncoder appendData:rcData];
    } else {
        self.firstPacketSend = NO; // change value for next call to this function
    }
    [super sendData:data]; /* NiFiTransaction */
}

- (void) cancel {
    [super cancel]; /* NiFiTransaction */
    [self.socket disconnect];
}

- (nullable NiFiTransactionResult *)confirmAndCompleteOrError:(NSError *_Nullable *_Nullable)error {
    self.transactionState = DATA_EXCHANGED;
    // 1. Send encoded flow files
    [self.socket writeData:self.dataPacketEncoder.getEncodedData withTimeout:self.config.timeout callback:nil];
    
    // 2. Send FINISH_TRANSACTION, Receive CRC checksum
    
    Byte finishTransactionBytes[] = {'R', 'C', FINISH_TRANSACTION};
    
    NSError *socketError;
    NSData *responseData = [self.socket readDataAfterWriteData:[NSData dataWithBytes:finishTransactionBytes length:3]
                                                       timeout:self.config.timeout
                                                         error:&socketError];
    
    if (socketError) {
        NSLog(@"Error: %@", socketError.localizedDescription);
        if (error) {
            *error = socketError;
        }
        [self error];
        return nil;
    }
    
    self.transactionState = TRANSACTION_FINISHED;
    
    NiFiTransactionResponseCode responseCode;
    NSString *responseMessage;
    BOOL success = [[self class] parseResponseCodeFromData:responseData responseCode:&responseCode message:&responseMessage];
    if (!success) {
        if (error) {
            *error = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorSiteToSiteTransactionInvalidServerResponse userInfo:nil];
        }
        [self error];
        return nil;
    }
    
    if (responseCode != CONFIRM_TRANSACTION) {
        [self error];
        return nil;
    }
    
    // 3. SEND CONFIRM_TRANSACTION to commit the flow files on the remote end
    self.transactionState = TRANSACTION_CONFIRMED;
    NiFiTransactionResult *transactionResult = [self endTransactionWithResponseCode:CONFIRM_TRANSACTION error:error];
    
    if (!transactionResult) {
        [self error];
        return nil;
    }
    
    transactionResult.duration = [[NSDate date] timeIntervalSinceDate:self.startTime];
    NSLog(@"Completed transaction. flowfiles_sent=%llu, transactionId=%@", transactionResult.dataPacketsTransferred, [self transactionId]);
    return transactionResult;
}

- (nullable NiFiTransactionResult *)endTransactionWithResponseCode:(NiFiTransactionResponseCode)responseCode
                                                             error:(NSError *_Nullable *_Nullable)error {
    
    NSMutableData *rcData = [NSMutableData data];
    Byte rcBytes[] = {'R', 'C', responseCode};
    [rcData appendBytes:rcBytes length:3];
    [rcData appendData:[[self class] javaUTFDataForString:@""]]; // empty message
    
    NSError *socketError;
    NSData *serverResponse = [self.socket readDataAfterWriteData:rcData timeout:self.config.timeout error:&socketError];
    
    if (socketError || !serverResponse) {
        if (socketError) {
            NSLog(@"Error: %@", socketError.localizedDescription);
            if (error) {
                *error = socketError;
            }
        }
        [self error];
        return nil;
    }
    
    NiFiTransactionResponseCode serverResponseCode;
    NSString *serverResponseMessage;
    BOOL successfullyParsedResponse = [[self class] parseResponseCodeFromData:serverResponse
                                                                 responseCode:&serverResponseCode
                                                                      message:&serverResponseMessage];
    
    if (!successfullyParsedResponse) {
        [self error];
        return nil;
    }
    
    [_socket writeData:[[self class] javaUTFDataForString:@"SHUTDOWN"] withTimeout:self.config.timeout callback:nil];
    self.transactionState = TRANSACTION_COMPLETED;
    NSTimeInterval transactionDuration = [[NSDate date] timeIntervalSinceDate:self.startTime];
    return [[NiFiTransactionResult alloc] initWithResponseCode:serverResponseCode
                                        dataPacketsTransferred:self.dataPacketEncoder.getDataPacketCount
                                                       message:serverResponseMessage
                                                      duration:transactionDuration];
}

+ (BOOL) parseResponseCodeFromData:(nonnull NSData *)data
                      responseCode:(nonnull NiFiTransactionResponseCode *)responseCodeOut
                           message:(NSString *_Nullable *_Nullable)messageOut {
    
    if (!data || data.length <= 0) {
        return NO;
    }
    
    NSUInteger dataLength = data.length;
    
    if (responseCodeOut) {
        Byte *responseBytes = (Byte *)[data bytes];
        if (dataLength < 3 || responseBytes[0] != 'R' || responseBytes[1] != 'C') {
            NSLog(@"Error parsing response code. Invalid data format.");
            return NO;
        }
        *responseCodeOut = responseBytes[2];
    }
    
    if (messageOut) {
        if (dataLength > 3) {
            NSData *messageData = [data subdataWithRange:NSMakeRange(3, dataLength-3)];
            if (messageOut) {
                *messageOut = [[self class] stringForjavaUTFData:messageData];
            }
        }
        else {
            *messageOut = nil;
        }
    }
    
    return YES;
}

/*! returns nifi://{peer.url.host}:{peer.rawPort} */
+ (NSURL *)getURLForPeer:(nonnull NiFiPeer *)peer {
    
    if (!peer) {
        return nil;
    }
    
    NSURLComponents *urlComponents = [NSURLComponents componentsWithURL:peer.url resolvingAgainstBaseURL:false];
    
    // protocol scheme is "nifi"
    urlComponents.scheme = @"nifi";
    
    // host is the same as peer.url.host, ie, nothing to do as we initialized components from peer.url
    
    // port needs to be explicitly set to peer.rawPort
    urlComponents.port = peer.rawPort;
    
    
    return urlComponents.URL;
}

+ (NSData *) javaUTFDataForString:(nonnull NSString*)str {
    NSUInteger len = [str lengthOfBytesUsingEncoding:NSUTF8StringEncoding];
    Byte buffer[2];
    buffer[0] = (0xff & (len >> 8));
    buffer[1] = (0xff & len);
    NSMutableData *outData = [NSMutableData dataWithCapacity:2];
    [outData appendBytes:buffer length:2];
    [outData appendData:[str dataUsingEncoding:NSUTF8StringEncoding]];
    return outData;
}

+ (NSString *) stringForjavaUTFData:(nonnull NSData *)data {
    if (!data || !data.length) {
        return nil;
    }
    const Byte *dataBytes = [data bytes];
    uint16_t len = (((dataBytes[0] & 0xff) >> 8) | (dataBytes[1] & 0xff));
    
    NSString *decodedString = [[NSString alloc] initWithData:[data subdataWithRange:NSMakeRange(2, len)]
                                                    encoding:NSUTF8StringEncoding];
    return decodedString;
}
                 
+ (void) appendInt32:(uint32_t)value toWireData:(NSMutableData *)data {
    // The server is Java, expecting a Java data stream. Java, and most platforms, use big endian / network order for wire data.
    uint32_t swappedValue = CFSwapInt32HostToBig(value);
    [data appendBytes:&swappedValue length:4];
}
                 

@end


@implementation NiFiSocketSiteToSiteClient

- (nullable NSObject <NiFiTransaction> *)createTransactionWithURLSession:(NSURLSession *)urlSession {
    
    [self updatePeersIfNecessary];
    NiFiPeer *peer = [self getPreferredPeer];
    

    NiFiHttpRestApiClient *restApiClient = [self createRestApiClientWithBaseUrl:peer.url
                                                                     urlSession:(NSObject<NSURLSessionProtocol> *)urlSession];
    
    if (!peer.rawPort) {
        NSError *s2sDiscoveryError;
        NSDictionary *siteToSiteInfo = [restApiClient getSiteToSiteInfoOrError:&s2sDiscoveryError];
        if (siteToSiteInfo && siteToSiteInfo[@"controller"]) {
            if (siteToSiteInfo[@"controller"][@"remoteSiteListeningPort"]) {
                peer.rawPort = siteToSiteInfo[@"controller"][@"remoteSiteListeningPort"];
                NSLog(@"Discovered raw port at peer. peer='%@', raw_port=%@", peer.url, peer.rawPort);
            }
            else {
                NSLog(@"Could not discover raw site to site port at peer. "
                      "Are you sure it is configured to perform site to site over the raw socket protocol?");
            }
        }
    }
    
    if (!self.prioritizedRemoteInputPortIdList) {
        [self updatePrioritizedPortList:restApiClient];
    }
    
    NiFiSocketTransaction *transaction = nil;
    if (self.prioritizedRemoteInputPortIdList && [self.prioritizedRemoteInputPortIdList count] > 0) {
        NSString *portId = self.prioritizedRemoteInputPortIdList[0];
        NSLog(@"Attempting to initiate transaction. portId=%@", portId);
        transaction = [[NiFiSocketTransaction alloc] initWithConfig:self.config
                                                remoteClusterConfig:self.remoteClusterConfig
                                                               peer:peer
                                                             portId:(NSString *)portId];
        if (transaction) {
            NSLog(@"Successfully initiated transaction. transactionId=%@, portId=%@",
                  transaction.transactionId, portId);
        }
    } else {
        NSLog(@"Could not discover remote s2s input portId. Please configure either portName or portId.");
    }
    
    if (!transaction) {
        [peer markFailure];
        self.isPeerUpdateNecessary = YES;
        NSLog(@"Could not create NiFi s2s transaction. Check NiFi s2s configuration. "
              "Is the correct url and s2s portName/portId set?");
    }
    return transaction;
    
    
}

@end












