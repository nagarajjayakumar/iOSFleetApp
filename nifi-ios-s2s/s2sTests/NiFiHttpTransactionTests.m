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
#import "NiFiSiteToSiteClient.h"
#import "NiFiHttpRestApiClient.h"

#define MOCK_SERVER_SIDE_TRANSACTION_TTL 4

@interface NiFiHttpTransactionTests : XCTestCase
@end


@interface MockHttpRestApiClient : NiFiHttpRestApiClient
@property NSInteger dataPacketsSentCount;
@property NSInteger ttlExtensionCallCount;
- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl;
@end


@implementation MockHttpRestApiClient : NiFiHttpRestApiClient

- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl {
    self = [super initWithBaseUrl:baseUrl];
    if (self) {
        _ttlExtensionCallCount = 0;
        _dataPacketsSentCount = 0;
    }
    return self;
}

- (nullable NSURL *)baseUrl {
    return [super baseUrl];
}

- (nullable NiFiTransactionResource *)initiateSendTransactionToPortId:(nonnull NSString *)portId
                                                                error:(NSError *_Nullable *_Nullable)error {
    NiFiTransactionResource *returnVal = [[NiFiTransactionResource alloc] init];
    returnVal.transactionId = @"new-test-transaction";
    returnVal.transactionUrl = [[NSURL URLWithString:@"transactions/new-test-transaction" relativeToURL:[super baseUrl]] absoluteString];
    returnVal.serverSideTtl = MOCK_SERVER_SIDE_TRANSACTION_TTL;
    returnVal.flowFilesSent = 0;
    returnVal.lastResponseCode = PROPERTIES_OK;
    return returnVal;
}

- (nullable NSString *)getPortIdForPortName:(nonnull NSString *)portName
                                      error:(NSError *_Nullable *_Nullable)error {
    return @"12345678-1234-1234-1234-1234567890abc";
}

- (void)extendTTLForTransaction:(nonnull NSString *)transactionUrl error:(NSError *_Nullable *_Nullable)error {
    _ttlExtensionCallCount++;
}

- (NSInteger)sendFlowFiles:(nonnull NiFiDataPacketEncoder *)dataPacketEncoder
           withTransaction:(nonnull NiFiTransactionResource *)transactionResource
                     error:(NSError *_Nullable *_Nullable)error {
    [dataPacketEncoder getEncodedData];
    _dataPacketsSentCount += [dataPacketEncoder getDataPacketCount];
    return [dataPacketEncoder getEncodedDataCrcChecksum];
}

- (nullable NiFiTransactionResult *)endTransaction:(nonnull NSString *)transactionUrl
                                      responseCode:(NiFiTransactionResponseCode)responseCode
                                             error:(NSError *_Nullable *_Nullable)error {
    NiFiTransactionResult *returnVal = [[NiFiTransactionResult alloc] initWithResponseCode:responseCode
                                                                    dataPacketsTransferred:_dataPacketsSentCount
                                                                                   message:nil
                                                                                  duration:10];
    return returnVal;
}

@end


@implementation NiFiHttpTransactionTests

- (void)testHttpTransaction {
    
    NSURL *baseURL = [NSURL URLWithString:@"http://hostname:port/nifi-api"];
    MockHttpRestApiClient *mockApiClient = [[MockHttpRestApiClient alloc] initWithBaseUrl:baseURL];
    
    NiFiHttpTransaction *transaction = [[NiFiHttpTransaction alloc] initWithPortId:@"testportid" httpRestApiClient:mockApiClient];
    XCTAssertEqual(TRANSACTION_STARTED, [transaction transactionState]);
    
    NSDictionary * attributes1 = @{@"packetNumber": @"1"};
    NSData * data1 = [@"Data Packet 1" dataUsingEncoding:NSUTF8StringEncoding];
    id dataPacket1 = [NiFiDataPacket dataPacketWithAttributes:attributes1 data:data1];
    [transaction sendData:dataPacket1];
    XCTAssertEqual(DATA_EXCHANGED, [transaction transactionState]);
    
    NiFiTransactionResult *transactionRsult = [transaction confirmAndCompleteOrError:nil];
    XCTAssertEqual(TRANSACTION_COMPLETED, [transaction transactionState]);
    XCTAssertEqual(1, [transactionRsult dataPacketsTransferred]);
}

- (void)testHttpTransactionKeepAlives {
    
    NSURL *baseURL = [NSURL URLWithString:@"http://hostname:port/nifi-api"];
    MockHttpRestApiClient *mockApiClient = [[MockHttpRestApiClient alloc] initWithBaseUrl:baseURL];
    
    NiFiHttpTransaction *transaction = [[NiFiHttpTransaction alloc] initWithPortId:@"testportid" httpRestApiClient:mockApiClient];
    
    uint sleepIntervalSeconds = MOCK_SERVER_SIDE_TRANSACTION_TTL;
    sleep(sleepIntervalSeconds);
    [transaction cancel];
    XCTAssertTrue(mockApiClient.ttlExtensionCallCount >= floor((double)MOCK_SERVER_SIDE_TRANSACTION_TTL / (double)sleepIntervalSeconds));
}

@end

