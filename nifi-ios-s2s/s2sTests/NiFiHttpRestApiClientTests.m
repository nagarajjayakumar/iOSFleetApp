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
#import <XCTest/XCTest.h>
#import "NiFiHttpRestApiClient.h"


@interface NiFiHttpRestApiClientTests : XCTestCase
@end


@interface MockResponse : NSObject
@property NSData *data;
@property NSURLResponse *response;
@property NSError *error;
@end


@interface MockURLSessionTask : NSURLSessionDataTask
@property (readwrite) MockResponse *mockResponse;
@property (readwrite) void (^completionHandler)(NSData *, NSURLResponse *, NSError *);
- (instancetype)initWithResponse:(MockResponse *)response
               completionHandler:(void (^_Null_unspecified)(NSData *_Nullable data, NSURLResponse *_Nullable response, NSError *_Nullable error))completionHandler;
@end


@interface MockURLSession : NSURLSession<NSURLSessionProtocol>
@property (readwrite) MockResponse *mockResponse;
- (instancetype)initWithResponse:(MockResponse *)response;
@end


@implementation MockResponse
// properties will be auto-synthesized
@end


@implementation MockURLSessionTask
- (instancetype)initWithResponse:(MockResponse *)response
               completionHandler:(void (^_Null_unspecified)(NSData *_Nullable data, NSURLResponse *_Nullable response, NSError *_Nullable error))completionHandler {
    self = [super init];
    if (self != nil) {
        _mockResponse = response;
        _completionHandler = completionHandler;
    }
    return self;
}

// Override resume functionality to return mock response
- (void) resume {
    _completionHandler(_mockResponse.data, _mockResponse.response, _mockResponse.error);
}
@end


@implementation MockURLSession

- (instancetype)initWithResponse:(MockResponse *)response {
    self = [super init];
    if (self != nil) {
        _mockResponse = response;
    }
    return self;
}

- (NSURLSessionDataTask *_Null_unspecified)dataTaskWithRequest:(NSURLRequest *_Null_unspecified)request
                                             completionHandler:(void (^_Null_unspecified)(NSData *_Nullable data, NSURLResponse *_Nullable response, NSError *_Nullable error))completionHandler {
    return [[MockURLSessionTask alloc] initWithResponse:_mockResponse completionHandler:completionHandler];
}

@end


@implementation NiFiHttpRestApiClientTests

- (void)setUp {
    [super setUp];
    // Put setup code here.
}

- (void)tearDown {
    // Put teardown code here.
    [super tearDown];
}

- (void)testInitiateSendTransactionToPortId {
    // Prepare mock URLSession to inject into NiFiHttpRestApiClient
    NSString *portId = @"82f79eb6-015c-1000-d191-ee1ef23b1a74";
    NSString *urlString = [@[ @"http://testhostname:8080/nifi-api/data-transfer/input-ports", portId, @"transactions"] componentsJoinedByString:@"/"];
    NSString *transactionId = @"8966b23c-1495-4c9e-9050-c0a2306122ce";
    NSString *transactionURL = [urlString stringByAppendingString:@"/8966b23c-1495-4c9e-9050-c0a2306122ce"];
    
    MockResponse *mockResponse = [[MockResponse alloc] init];
    mockResponse.response = [[NSHTTPURLResponse alloc] initWithURL:[NSURL URLWithString:urlString]
                                                        statusCode:201L
                                                       HTTPVersion:@"1.1"
                                                      headerFields:@{ @"Content-Type": @"application/json",
                                                                      @"Location": transactionURL,
                                                                      @"x-location-uri-intent": @"transaction-url",
                                                                      @"x-nifi-site-to-site-protocol-version": @"1",
                                                                      @"x-nifi-site-to-site-server-transaction-ttl": @"30" }];
    mockResponse.data = [@"{\"flowFileSent\":0,\"responseCode\":1,\"message\":\"Handshake properties are valid, and port is running. A transaction is created:8966b23c-1495-4c9e-9050-c0a2306122ce\"}" dataUsingEncoding:NSUnicodeStringEncoding];
    mockResponse.error = nil;
    NSObject<NSURLSessionProtocol> *mockURLSession = [[MockURLSession alloc] initWithResponse:mockResponse];
    
    // Init NiFiHttpRestApiClient using mock URLSession
    NSURL *baseApiURL = [NSURL URLWithString:@"http://testhostname:8080/nifi-api"];
    NiFiHttpRestApiClient *restApiClient = [[NiFiHttpRestApiClient alloc] initWithBaseUrl:baseApiURL
                                                                         clientCredential:nil
                                                                               urlSession:mockURLSession];
    
    // Use NiFiHttpRestApiClient to attempt to create transaction
    NSError *error;
    NiFiTransactionResource *tr = [restApiClient initiateSendTransactionToPortId:portId error:&error];
    
    // Assert excpeted behavior
    XCTAssertNil(error);
    XCTAssertNotNil(tr);
    
    XCTAssertNotNil(tr.transactionId);
    XCTAssertTrue([tr.transactionId isEqualToString:transactionId]);
    
    XCTAssertNotNil(tr.transactionUrl);
    XCTAssertTrue([tr.transactionUrl isEqualToString:transactionURL]);
    
    XCTAssertEqual(tr.serverSideTtl, 30);
    
    XCTAssertEqual(tr.flowFilesSent, 0);
    
    XCTAssertEqual(tr.lastResponseCode, PROPERTIES_OK);
    
    XCTAssertNotNil(tr.lastResponseMessage);
    XCTAssertTrue([tr.lastResponseMessage isEqualToString:@"Handshake properties are valid, and port is running. A transaction is created:8966b23c-1495-4c9e-9050-c0a2306122ce"]);
}

@end


