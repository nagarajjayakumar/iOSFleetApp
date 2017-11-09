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

//# define RUN_INTEGRATION_TESTS  // These are off by default as they require running an external NiFi server
# ifdef RUN_INTEGRATION_TESTS

#import <XCTest/XCTest.h>
#import "s2s.h"

@interface s2sTests : XCTestCase
@end

@implementation s2sTests

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}


- (void)testSiteToSiteUsage {
    
    NiFiSiteToSiteRemoteClusterConfig *remoteNiFiInstance =
        [NiFiSiteToSiteRemoteClusterConfig configWithUrl:[NSURL URLWithString:@"http://localhost:8080"]];
    NiFiSiteToSiteClientConfig *s2sConfig = [NiFiSiteToSiteClientConfig configWithRemoteCluster: remoteNiFiInstance];
    s2sConfig.portName = @"From iOS";
    
    id s2sClient = [NiFiSiteToSiteClient clientWithConfig:s2sConfig];
    
    id transaction = [s2sClient createTransaction];
    XCTAssertNotNil(transaction);
    XCTAssertEqual(TRANSACTION_STARTED, [transaction transactionState]);
    
    NSDictionary * attributes1 = @{@"packetNumber": @"1"};
    NSData * data1 = [@"Data Packet 1" dataUsingEncoding:NSUTF8StringEncoding];
    id dataPacket1 = [NiFiDataPacket dataPacketWithAttributes:attributes1 data:data1];
    [transaction sendData:dataPacket1];
    XCTAssertEqual(DATA_EXCHANGED, [transaction transactionState]);
    
    NSDictionary * attributes2 = @{@"packetNumber": @"2"};
    NSData * data2 = [@"Data Packet 2" dataUsingEncoding:NSUTF8StringEncoding];
    id dataPacket2 = [NiFiDataPacket dataPacketWithAttributes:attributes2 data:data2];
    [transaction sendData:dataPacket2];
    XCTAssertEqual(DATA_EXCHANGED, [transaction transactionState]);
    
    NSDictionary * attributes3 = @{@"packetNumber": @"3"};
    NSData * data3 = [@"Data Packet 3" dataUsingEncoding:NSUTF8StringEncoding];
    id dataPacket3 = [NiFiDataPacket dataPacketWithAttributes:attributes3 data:data3];
    [transaction sendData:dataPacket3];
    XCTAssertEqual(DATA_EXCHANGED, [transaction transactionState]);
    
    NiFiTransactionResult *transactionResult = [transaction confirmAndCompleteOrError:nil];
    XCTAssertEqual(TRANSACTION_COMPLETED, [transaction transactionState]);
    XCTAssertNotNil(transactionResult);
    XCTAssertEqual(3, transactionResult.dataPacketsTransferred);
}

- (void)testSiteToSiteTTLUsage {
    
    NiFiSiteToSiteRemoteClusterConfig *remoteNiFiInstance =
    [NiFiSiteToSiteRemoteClusterConfig configWithUrl:[NSURL URLWithString:@"http://localhost:8080"]];
    NiFiSiteToSiteClientConfig *s2sConfig = [NiFiSiteToSiteClientConfig configWithRemoteCluster: remoteNiFiInstance];
    s2sConfig.portName = @"From iOS";
    
    id s2sClient = [NiFiSiteToSiteClient clientWithConfig:s2sConfig];
    
    id transaction = [s2sClient createTransaction];
    XCTAssertNotNil(transaction);
    XCTAssertEqual(TRANSACTION_STARTED, [transaction transactionState]);
    
    unsigned int sleepSeconds = 45;
    NSLog(@"Sleeping for %u seconds to test ttl.", sleepSeconds);
    sleep(sleepSeconds);
    NSLog(@"Done sleeping. Sending data packets.");
    
    NSDictionary * attributes1 = @{@"packetNumber": @"1"};
    NSData * data1 = [@"Data Packet 1" dataUsingEncoding:NSUTF8StringEncoding];
    id dataPacket1 = [NiFiDataPacket dataPacketWithAttributes:attributes1 data:data1];
    [transaction sendData:dataPacket1];
    XCTAssertEqual(DATA_EXCHANGED, [transaction transactionState]);
    
    NSDictionary * attributes2 = @{@"packetNumber": @"2"};
    NSData * data2 = [@"Data Packet 2" dataUsingEncoding:NSUTF8StringEncoding];
    id dataPacket2 = [NiFiDataPacket dataPacketWithAttributes:attributes2 data:data2];
    [transaction sendData:dataPacket2];
    XCTAssertEqual(DATA_EXCHANGED, [transaction transactionState]);
    
    NSDictionary * attributes3 = @{@"packetNumber": @"3"};
    NSData * data3 = [@"Data Packet 3" dataUsingEncoding:NSUTF8StringEncoding];
    id dataPacket3 = [NiFiDataPacket dataPacketWithAttributes:attributes3 data:data3];
    [transaction sendData:dataPacket3];
    XCTAssertEqual(DATA_EXCHANGED, [transaction transactionState]);
    
    NiFiTransactionResult *transactionResult = [transaction confirmAndCompleteOrError:nil];
    XCTAssertEqual(TRANSACTION_COMPLETED, [transaction transactionState]);
    XCTAssertNotNil(transactionResult);
    XCTAssertEqual(3, transactionResult.dataPacketsTransferred);
}

@end

# endif // RUN_INTEGRATION_TESTS
