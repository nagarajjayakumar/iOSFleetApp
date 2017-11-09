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
#import "NiFiHttpRestApiClient.h"
#import "NiFiSiteToSiteTransaction.h"
#import "NiFiError.h"

#define DEFAULT_HTTP_TIMEOUT 15.0

static NSString *const HTTP_SITE_TO_SITE_PROTOCOL_VERSION = @"5";

static NSString *const HTTP_HEADER_PROTOCOL_VERSION = @"x-nifi-site-to-site-protocol-version";
static NSString *const HTTP_HEADER_SERVER_SIDE_TRANSACTION_TTL = @"x-nifi-site-to-site-server-transaction-ttl";
static NSString *const HTTP_HEADER_HANDSHAKE_PROPERTY_USE_COMPRESSION = @"x-nifi-site-to-site-use-compression";
static NSString *const HTTP_HEADER_HANDSHAKE_PROPERTY_REQUEST_EXPIRATION = @"x-nifi-site-to-site-request-expiration";
static NSString *const HTTP_HEADER_HANDSHAKE_PROPERTY_BATCH_COUNT = @"x-nifi-site-to-site-batch-count";
static NSString *const HTTP_HEADER_HANDSHAKE_PROPERTY_BATCH_SIZE = @"x-nifi-site-to-site-batch-size";
static NSString *const HTTP_HEADER_HANDSHAKE_PROPERTY_BATCH_DURATION = @"x-nifi-site-to-site-batch-duration";
static NSString *const HTTP_HEADER_LOCATION = @"Location";
static NSString *const HTTP_HEADER_LOCATION_URI_INTENT_NAME = @"x-location-uri-intent";
static NSString *const HTTP_HEADER_LOCATION_URI_INTENT_VALUE = @"transaction-url";


/********** TransactionResource **********/

@interface NiFiTransactionResource()
- (nullable NSMutableURLRequest *) flowFilesUrlRequest;
@end

@implementation NiFiTransactionResource

- (nonnull instancetype)initWithTransactionId:(nonnull NSString *)transactionId {
    self = [super init];
    if(self != nil) {
        _transactionId = transactionId;
        _transactionUrl = nil;
        _serverSideTtl = -1;
        _flowFilesSent = 0;
        _lastResponseCode = RESERVED;
        _lastResponseMessage = nil;
    }
    return self;
}

- (nullable NSMutableURLRequest *) flowFilesUrlRequest {
    NSString *urlStr = [_transactionUrl stringByAppendingString:@"/flow-files"];
    NSURL *url = [NSURL URLWithString:urlStr];
    
    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:url
                                                           cachePolicy:NSURLRequestUseProtocolCachePolicy
                                                       timeoutInterval:DEFAULT_HTTP_TIMEOUT];
    [request setHTTPMethod:@"POST"];
    
    NSDictionary *headers = @{@"Content-Type": @"application/octet-stream",
                              HTTP_HEADER_PROTOCOL_VERSION: HTTP_SITE_TO_SITE_PROTOCOL_VERSION};
    [request setAllHTTPHeaderFields:headers];
    
    return request;
}

@end


/********** HttpRestApiClient **********/

@interface NiFiHttpRestApiClient()
@property (nonatomic, retain, readwrite, nonnull) NSURLComponents *baseUrlComponents;
@property (nonatomic, retain, nonnull) NSObject<NSURLSessionProtocol> *urlSession;
@property (nonatomic, retain, readwrite, nullable) NSURLCredential *credential;
@property (nonatomic, retain, readwrite, nullable) NSString *authToken;
@property (nonatomic, retain, readwrite, nullable) NSDate *authExpiration;
@end

@implementation NiFiHttpRestApiClient

// MARK: - Constructors

- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl {
    return [self initWithBaseUrl:baseUrl
                    clientCredential:nil];
}

- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl
                        clientCredential:(nullable NSURLCredential *)credendtial {
    return [self initWithBaseUrl:baseUrl
                clientCredential:credendtial
                      urlSession:(NSObject<NSURLSessionProtocol> *)[NSURLSession sharedSession]];
}

- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl
                        clientCredential:(nullable NSURLCredential *)credendtial
                              urlSession:(nonnull NSObject<NSURLSessionProtocol> *)urlSession {
    self = [super init];
    if(self != nil) {
        _urlSession = urlSession;
        _baseUrlComponents = [NSURLComponents componentsWithURL:baseUrl resolvingAgainstBaseURL:false];
        _credential = credendtial;
        _authToken = nil;
        
        // Set base url path if none is specified
        if (nil == _baseUrlComponents.path || [_baseUrlComponents.path isEqualToString:@""]) {
            _baseUrlComponents.path = @"/nifi-api";
        }
    }
    return self;
}

// MARK: - Accessors

- (nullable NSURL *)baseUrl {
    return _baseUrlComponents.URL;
}

// MARK: - Discovery

- (nullable NSDictionary *)getSiteToSiteInfoOrError:(NSError *_Nullable *_Nullable)error {
    NSURLComponents * urlComponents = [_baseUrlComponents copy];
    urlComponents.path = [NSString stringWithFormat:@"%@/site-to-site", urlComponents.path];
    NSURL *url = urlComponents.URL;
    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:url
                                                           cachePolicy:NSURLRequestUseProtocolCachePolicy
                                                       timeoutInterval:DEFAULT_HTTP_TIMEOUT];
    [request setHTTPMethod:@"GET"];
    
    NSDictionary *headers = @{@"Accept": @"application/json"};
    [request setAllHTTPHeaderFields:headers];
    
    [self addAuthTokenHeaderToRequest:&request error:error];
    
    NSData *data;
    NSHTTPURLResponse *response;
    NSError *dataTaskError;
    
    [self synchronousDataTaskWithRequest:request
                              dataOutput:&data
                          responseOutput:&response
                             errorOutput:&dataTaskError];
    
    if (response == nil) {
        if (error) {
            *error = dataTaskError ?: [NSError errorWithDomain:NiFiErrorDomain
                                                          code:NiFiErrorSiteToSiteClientCouldNotLookupSiteToSiteInfo
                                                      userInfo:nil];
        }
        NSLog(@"Unable to discover site-to-site info. Error communicating with peer.");
        return nil;
    } else if (response.statusCode != 200) {
        if (error) {
            *error = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorHttpStatusCode + response.statusCode userInfo:nil];
        }
        NSLog(@"Unable to discover site-to-site info. Server returned status code '%ld'.", (long)response.statusCode);
        return nil;
    }
    
    // Response body should be JSON with site-to-site info
    NSError *jsonError;
    NSDictionary *siteToSiteInfo = [NSJSONSerialization JSONObjectWithData:data
                                                                   options:0
                                                                     error:&jsonError];
    if (jsonError) {
        if (error) {
            *error = jsonError;
            NSLog(@"Unable to discover site-to-site info. Error deserializing JSON response.");
            return nil;
        }
    }
    
    return siteToSiteInfo;
}

- (nullable NSDictionary *)getRemoteInputPortsOrError:(NSError *_Nullable *_Nullable)error {
    
    NSError *siteToSiteInfoError;
    NSDictionary *siteToSiteInfo = [self getSiteToSiteInfoOrError:&siteToSiteInfoError];
    
    if (!siteToSiteInfo) {
        if (siteToSiteInfoError && *error) {
            *error = siteToSiteInfoError;
        }
        return nil;
    }

    NSMutableDictionary *portIdsByName = nil;
    if (siteToSiteInfo && siteToSiteInfo[@"controller"]) {
        NSArray *inputPorts = siteToSiteInfo[@"controller"][@"inputPorts"];
        if (inputPorts) {
            portIdsByName = [NSMutableDictionary dictionary];
            for (NSDictionary *inputPort in inputPorts) {
                if (inputPort[@"id"] && inputPort[@"name"]) {
                    NSString *existingIdValue = [portIdsByName objectForKey:inputPort[@"name"]];
                    if (!existingIdValue) {
                        [portIdsByName setValue:inputPort[@"id"] forKey:inputPort[@"name"]];
                    } else {
                        NSLog(@"WARNING: NiFI peer API reporting duplicate input ports named '%@'. '%@' and '%@' both found. Using '%@'",
                              inputPort[@"name"],
                              existingIdValue, inputPort[@"id"],
                              existingIdValue);
                    }
                }
            }
        }
    }
    if (!portIdsByName) {
        if (error) {
            *error = [NSError errorWithDomain:NiFiErrorDomain
                                         code:NiFiErrorSiteToSiteClientCouldNotLookupInputPorts userInfo:nil];
        }
        NSLog(@"Unable to discover remote input ports. No input ports found in JSON response. Possible protocol error.");
        return nil;
    }
    
    return portIdsByName;
}

- (nullable NSArray<NiFiPeer *> *)getPeersOrError:(NSError *_Nullable *_Nullable)error {
    
    NSURLComponents * urlComponents = [_baseUrlComponents copy];
    urlComponents.path = [NSString stringWithFormat:@"%@/site-to-site/peers", urlComponents.path];
    NSURL *url = urlComponents.URL;
    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:url
                                                           cachePolicy:NSURLRequestUseProtocolCachePolicy
                                                       timeoutInterval:DEFAULT_HTTP_TIMEOUT];
    [request setHTTPMethod:@"GET"];
    
    NSDictionary *headers = @{@"Accept": @"application/json",
                              HTTP_HEADER_PROTOCOL_VERSION: HTTP_SITE_TO_SITE_PROTOCOL_VERSION};
    [request setAllHTTPHeaderFields:headers];
    
    [self addAuthTokenHeaderToRequest:&request error:error];
    
    NSData *data;
    NSHTTPURLResponse *response;
    NSError *dataTaskError;
    
    [self synchronousDataTaskWithRequest:request
                              dataOutput:&data
                          responseOutput:&response
                             errorOutput:&dataTaskError];
    
    if (response == nil) {
        if (error) {
            *error = dataTaskError ?: [NSError errorWithDomain:NiFiErrorDomain
                                                          code:NiFiErrorSiteToSiteClientCouldNotLookupPeers
                                                      userInfo:nil];
        }
        NSLog(@"Unable to discover peers in remote cluster.");
        return nil;
    } else if (response.statusCode != 200) {
        if (error) {
            *error = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorHttpStatusCode + response.statusCode userInfo:nil];
        }
        NSLog(@"Unable to discover peers in remote cluster. Server returned status code '%ld'.", (long)response.statusCode);
        return nil;
    }
    
    // Response body should be JSON with site-to-site info
    NSError *jsonError;
    NSDictionary *bodyJson = [NSJSONSerialization JSONObjectWithData:data
                                                             options:0
                                                               error:&jsonError];
    if (jsonError) {
        if (error) {
            *error = jsonError;
            NSLog(@"Unable to discover peers in remote cluster. Error deserializing JSON response.");
            return nil;
        }
    }
    
    NSMutableArray *peers = nil;
    if (bodyJson && bodyJson[@"peers"]) {
        peers = [NSMutableArray arrayWithCapacity:[bodyJson[@"peers"] count]];
        for (NSDictionary *peerJson in bodyJson[@"peers"]) {
            
            NiFiPeer *peer = nil;
            if (peerJson[@"hostname"]) {
                
                NSURLComponents *peerUrlComponents = [[NSURLComponents alloc] init];
                peerUrlComponents.host = peerJson[@"hostname"];
                peerUrlComponents.port = peerJson[@"port"];
                BOOL isSecurePeer = (peerJson[@"secure"] && [peerJson[@"secure"] boolValue]);
                peerUrlComponents.scheme = isSecurePeer ? @"https" : @"http";
                NSURL *peerUrl = peerUrlComponents.URL;
                if (peerUrl) {
                    peer = [NiFiPeer peerWithUrl:peerUrl];
                    if (peerJson[@"flowFileCount"]) {
                        peer.flowFileCount = [peerJson[@"flowFileCount"] integerValue];
                    }
                    [peers addObject:peer];
                }
            }
        }
    }
    if (!peers) {
        if (error) {
            *error = [NSError errorWithDomain:NiFiErrorDomain
                                         code:NiFiErrorSiteToSiteClientCouldNotLookupPeers
                                     userInfo:nil];
        }
        NSLog(@"Unable to discover peers in remote cluster. No peers found in JSON response. Possible protocol error.");
        return nil;
    }
    
    return peers;
}

// MARK: - S2S HTTP Transaction

- (nullable NiFiTransactionResource *)initiateSendTransactionToPortId:(nonnull NSString *)portId
                                                                error:(NSError **)error {
    
    NSURLComponents * urlComponents = [_baseUrlComponents copy];
    urlComponents.path = [NSString stringWithFormat:@"%@/data-transfer/input-ports/%@/transactions", urlComponents.path, portId];
    NSURL * url = urlComponents.URL;
    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:url
                                                           cachePolicy:NSURLRequestUseProtocolCachePolicy
                                                       timeoutInterval:DEFAULT_HTTP_TIMEOUT];
    [request setHTTPMethod:@"POST"];
    
    NSDictionary *headers = @{@"Content-Type": @"application/json",
                              @"Accept": @"application/json",
                              HTTP_HEADER_PROTOCOL_VERSION: HTTP_SITE_TO_SITE_PROTOCOL_VERSION};
    [request setAllHTTPHeaderFields:headers];
    
    [self addAuthTokenHeaderToRequest:&request error:error];
    
    NSData *data;
    NSHTTPURLResponse *response;
    NSError *dataTaskError;
    
    [self synchronousDataTaskWithRequest:request
                              dataOutput:&data
                          responseOutput:&response
                             errorOutput:&dataTaskError];
    
    NiFiTransactionResource *transactionResource = nil;
    if(!dataTaskError) {
        switch (response.statusCode) {
            case 200: // applying Postel's Principle to server response code
            case 201: {
                // Process response headers
                NSDictionary *headers = response.allHeaderFields;
                NSString *locationUriIntent = [headers objectForKey:HTTP_HEADER_LOCATION_URI_INTENT_NAME];
                if (locationUriIntent && [locationUriIntent isEqualToString:HTTP_HEADER_LOCATION_URI_INTENT_VALUE]) {
                    NSString *transactionUrl = [headers objectForKey:HTTP_HEADER_LOCATION];
                    NSString *transactionId = [[transactionUrl componentsSeparatedByString:@"/"] lastObject];
                    
                    if (transactionId) {
                        transactionResource = [[NiFiTransactionResource alloc] initWithTransactionId:transactionId];
                        transactionResource.transactionUrl = transactionUrl;
                        
                        NSString *serverSideTtl = [headers objectForKey:HTTP_HEADER_SERVER_SIDE_TRANSACTION_TTL];
                        if (serverSideTtl) {
                            transactionResource.serverSideTtl = [serverSideTtl integerValue];
                        }
                        
                        // Process response body, which we expect to be in the form:
                        // {"flowFileSent":0,
                        //  "responseCode":1,
                        //   "message":"Handshake properties are valid, and port is running.\
                        //              A transaction is created:XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
                        // }
                        NSError *jsonError;
                        NSDictionary *transactionJson = [NSJSONSerialization JSONObjectWithData:data options:kNilOptions error:&jsonError];
                        if (!jsonError) {
                            //flowFileSent
                            NSNumber *flowFileSent = [transactionJson objectForKey:@"flowFileSent"];
                            if (flowFileSent) {
                                transactionResource.flowFilesSent = [flowFileSent unsignedIntegerValue];
                            }
                            //responseCode
                            NSNumber *responseCode = [transactionJson objectForKey:@"responseCode"];
                            if (responseCode) {
                                transactionResource.lastResponseCode = (NiFiTransactionResponseCode)[responseCode integerValue];
                            }
                            //message
                            transactionResource.lastResponseMessage = [transactionJson objectForKey:@"message"];
                        } else {
                            // Note parsing the body can fail but if the response code was 201 the transaction was still created.
                            // We will log it and return a transaction and an error output.
                            *error = jsonError;
                        }
                    }
                }
                
                break;
            }
            default: {
                NSMutableDictionary *errorDetail = [NSMutableDictionary dictionary];
                NSString *localizedDescription = [NSString stringWithFormat:@"Server responded with HTTP status code %ld", (long)response.statusCode];
                [errorDetail setValue:localizedDescription forKey:NSLocalizedDescriptionKey];
                *error = [NSError errorWithDomain:@"NiFiSiteToSite" code:100 userInfo:errorDetail];
            }
        }
    } else {
        *error = dataTaskError;
    }
    
    if (!transactionResource || !transactionResource.transactionUrl) {
        return nil;
    }
    return transactionResource;
}

- (void)extendTTLForTransaction:(nonnull NSString *)transactionUrl error:(NSError **)error {
    NSURL *url = [NSURL URLWithString:transactionUrl];
    NSMutableURLRequest *ttlExtendRequest = [NSMutableURLRequest requestWithURL:url
                                                                    cachePolicy:NSURLRequestUseProtocolCachePolicy
                                                                timeoutInterval:DEFAULT_HTTP_TIMEOUT];
    [ttlExtendRequest setHTTPMethod:@"PUT"];
    
    NSDictionary *headers = @{HTTP_HEADER_PROTOCOL_VERSION: HTTP_SITE_TO_SITE_PROTOCOL_VERSION};
    [ttlExtendRequest setAllHTTPHeaderFields:headers];
    
    [self addAuthTokenHeaderToRequest:&ttlExtendRequest error:error];
    
    NSData *data;
    NSHTTPURLResponse *response;
    
    [self synchronousDataTaskWithRequest:ttlExtendRequest
                              dataOutput:&data
                          responseOutput:&response
                             errorOutput:error];
    
    if (response != nil) {
        if (response.statusCode < 200 || response.statusCode > 299) {
            if(error) {
                *error = [NSError errorWithDomain:NiFiErrorDomain
                                             code:NiFiErrorHttpStatusCode + response.statusCode
                                         userInfo:nil];
            }
            // NSLog(@"Extending TTL failed for transaction. transactionURL=%@, responseCode=%ld", transactionUrl, (long)response.statusCode);
        }
        else {
            // NSLog(@"Successfully extended TTL for transaction. transactionURL=%@, responseCode=%ld", transactionUrl, (long)response.statusCode);
        }
    }
}

- (NSInteger)sendFlowFiles:(nonnull NiFiDataPacketEncoder *)dataPacketEncoder
            withTransaction:(nonnull NiFiTransactionResource *)transactionResource
                      error:(NSError *_Nullable *_Nullable)error {
    
    NSMutableURLRequest *flowFilesRequest = [transactionResource flowFilesUrlRequest];
    
    if (!flowFilesRequest) {
        if (error) {
            *error = [NSError errorWithDomain:NiFiErrorDomain
                                         code:NiFiErrorHttpRestApiClientCouldNotFormURL
                                     userInfo:nil];
        }
    }
    
    [self addAuthTokenHeaderToRequest:&flowFilesRequest error:error];
    
    [flowFilesRequest setHTTPBodyStream:[dataPacketEncoder getEncodedDataStream]];
    
    NSData *data;
    NSHTTPURLResponse *response;
    NSError *dataTaskError;
    
    [self synchronousDataTaskWithRequest:flowFilesRequest
                              dataOutput:&data
                          responseOutput:&response
                             errorOutput:&dataTaskError];
    
    if (error && response == nil) {
        *error = dataTaskError;
        return -1;
    }
    
    switch (response.statusCode) {
        case 200: // applying Postel's Principle to server response code
        case 202:
        {
            // Response body should be server-calculated CRC checksum
            NSString *responseBody = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
            return [responseBody integerValue];
        }
        default:
            if (error) {
                *error = [NSError errorWithDomain:NiFiErrorDomain
                                             code:NiFiErrorHttpStatusCode + response.statusCode
                                         userInfo:nil];
            }
            return -1;
    }
    
}

- (nullable NiFiTransactionResult *)endTransaction:(nonnull NSString *)transactionUrl
                                     responseCode:(NiFiTransactionResponseCode)responseCode
                                            error:(NSError *_Nullable *_Nullable)error {
    NSURLComponents *urlComponents = [NSURLComponents componentsWithString:transactionUrl];
    
    NSMutableArray *queryItems = urlComponents.queryItems != nil ? [[NSMutableArray alloc] initWithArray:urlComponents.queryItems] : [[NSMutableArray alloc] initWithCapacity:1];
    [queryItems addObject:[NSURLQueryItem queryItemWithName:@"responseCode" value:[NSString stringWithFormat:@"%u", responseCode]]];
    urlComponents.queryItems = queryItems;
    
    NSURL *url = urlComponents.URL;
    NSMutableURLRequest *request = [NSMutableURLRequest requestWithURL:url
                                                           cachePolicy:NSURLRequestUseProtocolCachePolicy
                                                       timeoutInterval:DEFAULT_HTTP_TIMEOUT]; 
    [request setHTTPMethod:@"DELETE"];
    
    NSDictionary *headers = @{@"Content-Type": @"application/octet-stream",
                              HTTP_HEADER_PROTOCOL_VERSION: HTTP_SITE_TO_SITE_PROTOCOL_VERSION};
    [request setAllHTTPHeaderFields:headers];
    
    [self addAuthTokenHeaderToRequest:&request error:error];
    
    NSData *data;
    NSHTTPURLResponse *response;
    NSError *dataTaskError;
    
    [self synchronousDataTaskWithRequest:request
                              dataOutput:&data
                          responseOutput:&response
                             errorOutput:&dataTaskError];
    
    if (response == nil) {
        if (error) {
            *error = dataTaskError;
        }
        return nil;
    }
    
    NSError *jsonParseError;
    NSDictionary *transactionResultJson = [NSJSONSerialization JSONObjectWithData:data
                                                                          options:NSJSONReadingMutableContainers
                                                                            error:&jsonParseError];
    if (!transactionResultJson) {
        *error = jsonParseError;
        return nil;
    }
    
    NiFiTransactionResult *transactionResult = [[NiFiTransactionResult alloc] init];
    NSString *flowFileSentVal = transactionResultJson[@"flowFileSent"];
    NSString *responseCodeVal = transactionResultJson[@"responseCode"];
    transactionResult.message = transactionResultJson[@"message"];
    if (flowFileSentVal) {
        transactionResult.dataPacketsTransferred = [flowFileSentVal integerValue];
    }
    if (responseCodeVal) {
        transactionResult.responseCode = (NiFiTransactionResponseCode)[responseCodeVal integerValue];
    }
    return transactionResult;
}

// MARK: - Helper functions

/* A call to this method will block.
 * It is only desinged to be called from a background thread, not a UI thread. */
- (void) synchronousDataTaskWithRequest:(NSURLRequest *_Nonnull)request
                             dataOutput:(NSData *_Nullable *_Nonnull)data
                         responseOutput:(NSURLResponse *_Nullable *_Nonnull)response
                            errorOutput:(NSError *_Nullable *_Nullable)error {
    __block NSData * blockData = nil;
    __block NSURLResponse * blockResponse = nil;
    __block NSError * blockError = nil;
    
    dispatch_semaphore_t semaphore = dispatch_semaphore_create(0);
    NSURLSessionDataTask *dataTask = [self.urlSession dataTaskWithRequest:request completionHandler:^(NSData *d, NSURLResponse *r, NSError *e) {
        blockData = d;
        blockResponse = r;
        blockError = e;
        dispatch_semaphore_signal(semaphore);
    }];
    [dataTask resume];
    dispatch_time_t timeout = dispatch_time(DISPATCH_TIME_NOW, request.timeoutInterval * NSEC_PER_SEC);
    long didTimeout = dispatch_semaphore_wait(semaphore, timeout);
    
    if(!didTimeout) {
        *data = blockData;
        *response = blockResponse;
        if (error) {
            *error = blockError;
        }
    }
    else {
        if (error) {
            *error = [NSError errorWithDomain:NSURLErrorDomain code:NSURLErrorTimedOut userInfo:nil];
        }
    }
}

- (void)addAuthTokenHeaderToRequest:(NSMutableURLRequest **)request
                              error:(NSError **)error {
    if (_credential) {
        NSDate *startTime = [NSDate date];
        if (!_authToken || !_authExpiration || [startTime compare:_authExpiration] == NSOrderedDescending) {
            NSString *user = _credential.user;
            NSString *password = _credential.password; // may prompt user
            
            if (user && password) {
                NSURLComponents * urlComponents = [_baseUrlComponents copy];
                urlComponents.path = [NSString stringWithFormat:@"%@/access/token", urlComponents.path];
                NSMutableURLRequest *authTokenRequest = [NSMutableURLRequest requestWithURL:urlComponents.URL
                                                                                cachePolicy:NSURLRequestReloadIgnoringCacheData
                                                                            timeoutInterval:DEFAULT_HTTP_TIMEOUT];
                [authTokenRequest setHTTPMethod:@"POST"];
                
                NSString *formData = [NSString stringWithFormat:@"username=%@&password=%@", user, password];
                NSData *encodedFormData = [formData dataUsingEncoding:NSASCIIStringEncoding allowLossyConversion:YES];
                NSString *contentLength = [NSString stringWithFormat:@"%lu", (unsigned long)encodedFormData.length];
                [authTokenRequest setHTTPBody:encodedFormData];
                
                NSDictionary *headers = @{@"Accept": @"text/plain",
                                          @"Content-Type": @"application/x-www-form-urlencoded",
                                          @"Content-Length": contentLength};
                [authTokenRequest setAllHTTPHeaderFields:headers];
                
                NSData *data;
                NSHTTPURLResponse *response;
                
                [self synchronousDataTaskWithRequest:authTokenRequest
                                          dataOutput:&data
                                      responseOutput:&response
                                         errorOutput:error];
                
                if (response == nil) {
                    _authToken = nil;
                    return;
                }
                
                if (response.statusCode < 200 || response.statusCode > 299) {
                    _authToken = nil;
                    return;
                }
                
                // Response body should be JWT in form base64(header).base64(payload).base64(signature)
                NSString *responseBody = [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding];
                if (responseBody) {
                    _authToken = [@"Bearer " stringByAppendingString:responseBody];
                } else {
                    _authToken = nil;
                }
                
                // Determine expiry
                NSArray *jwtComponents = [responseBody componentsSeparatedByString:@"."];
                NSString *base64EncodedJWTPayload = jwtComponents[1];
                int padLength = (4 - (base64EncodedJWTPayload.length % 4)) % 4;
                NSString *paddedBase64EncodedJWTPayload = [NSString stringWithFormat:@"%s%.*s", [base64EncodedJWTPayload UTF8String], padLength, "=="];
                NSData *decodedJWTPayload = [[NSData alloc] initWithBase64EncodedString:paddedBase64EncodedJWTPayload options:0];
                NSDictionary *decodedJson = [NSJSONSerialization JSONObjectWithData:decodedJWTPayload
                                                                            options:NSJSONReadingMutableContainers
                                                                              error:error];
                if (decodedJson) {
                    NSInteger exp = [decodedJson[@"exp"] integerValue];
                    NSInteger iat = [decodedJson[@"iat"] integerValue];
                    NSTimeInterval validDuration = ((double)exp - (double)iat) - 30.0; // seconds.
                    if (validDuration < 0.0) {
                        NSLog(@"Authentication token valid duration is < 30 seconds");
                        _authToken = nil;
                    }
                    _authExpiration = [NSDate dateWithTimeInterval:validDuration sinceDate:startTime];
                }
            }
        }
        if (_authToken) {
            [*request setValue:_authToken forHTTPHeaderField:@"Authorization"];
        }
    }
}

@end

