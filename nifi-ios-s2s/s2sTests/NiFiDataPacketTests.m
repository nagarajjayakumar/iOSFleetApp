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


@interface NiFiDataPacketTests : XCTestCase
@end

@implementation NiFiDataPacketTests

- (void)setUp {
    [super setUp];
    // Put setup code here.
}

- (void)tearDown {
    // Put teardown code here.
    [super tearDown];
}

- (void)testNoData {
    NSDictionary *attributes = @{ @"key1": @"value1", @"key2": @"value2" };
    NiFiDataPacket *testDataPacket = [NiFiDataPacket dataPacketWithAttributes:attributes data:nil];
    
    XCTAssertNotNil(testDataPacket);
    XCTAssertNotNil([testDataPacket attributes]);
    XCTAssertEqual(2, [[testDataPacket attributes] count]);
    XCTAssertNil([testDataPacket data]);
    XCTAssertNil([testDataPacket dataStream]);
    XCTAssertEqual(0, [testDataPacket dataLength]);
}

- (void)testDataAttributesFactoryMethod {
    NSDictionary *attributes = @{ @"key1": @"value1", @"key2": @"value2" };
    NSData *data = [@"test" dataUsingEncoding:NSUTF8StringEncoding];
    NiFiDataPacket *testDataPacket = [NiFiDataPacket dataPacketWithAttributes:attributes data:data];
    
    XCTAssertNotNil(testDataPacket);
    XCTAssertNotNil([testDataPacket attributes]);
    XCTAssertEqual(2, [[testDataPacket attributes] count]);
    XCTAssertNotNil([testDataPacket data]);
    XCTAssertNotNil([testDataPacket dataStream]);
    XCTAssertEqual(4, [testDataPacket dataLength]);
}

- (void)testStringFactoryMethod {
    NiFiDataPacket *testDataPacket = [NiFiDataPacket dataPacketWithString:@"test"];
    [testDataPacket setAttributeValue:@"value1" forAttributeKey:@"key1"];
    
    XCTAssertNotNil(testDataPacket);
    XCTAssertNotNil([testDataPacket attributes]);
    XCTAssertEqual(1, [[testDataPacket attributes] count]);
    XCTAssertNotNil([testDataPacket data]);
    XCTAssertNotNil([testDataPacket dataStream]);
    XCTAssertEqual(4, [testDataPacket dataLength]);
}

- (void)testInputStreamFactoryMethod {
    // Create temporary file from which to create an input stream
    NSString *fileName = [NSString stringWithFormat:@"%@_%@", [[NSProcessInfo processInfo] globallyUniqueString], @"testfile1.txt"];
    NSString *filePath = [NSTemporaryDirectory() stringByAppendingPathComponent:fileName];
    NSData *data = [@"test" dataUsingEncoding:NSUTF8StringEncoding];
    [data writeToFile:filePath atomically:YES];

    // Create data packet wrapper for input stream from file
    NSDictionary *attributes = @{ @"filename": fileName };
    NSInputStream *dataStream = [NSInputStream inputStreamWithFileAtPath:filePath];
    NiFiDataPacket *testDataPacket = [NiFiDataPacket dataPacketWithAttributes:attributes dataStream:dataStream dataLength:4];
    
    XCTAssertNotNil(testDataPacket);
    XCTAssertNotNil([testDataPacket attributes]);
    XCTAssertEqual(1, [[testDataPacket attributes] count]);
    XCTAssertNotNil([testDataPacket dataStream]);
    XCTAssertEqual(4, [testDataPacket dataLength]);
    
    [[NSFileManager defaultManager] removeItemAtPath:filePath error:nil];
}

- (void)testFileFactoryMethod {
    // Create temporary file from which to create an input stream
    NSString *fileName = [NSString stringWithFormat:@"%@_%@", [[NSProcessInfo processInfo] globallyUniqueString], @"testfile2.txt"];
    NSString *filePath = [NSTemporaryDirectory() stringByAppendingPathComponent:fileName];
    NSData *data = [@"test" dataUsingEncoding:NSUTF8StringEncoding];
    [data writeToFile:filePath atomically:YES];
    
    // Create data packet wrapper for input stream from file using file factory method
    NiFiDataPacket *testDataPacket = [NiFiDataPacket dataPacketWithFileAtPath:filePath];
    [testDataPacket setAttributeValue:@"value1" forAttributeKey:@"key1"];
    
    XCTAssertNotNil(testDataPacket);
    XCTAssertNotNil([testDataPacket attributes]);
    XCTAssertEqual(1, [[testDataPacket attributes] count]);
    XCTAssertNotNil([testDataPacket dataStream]);
    XCTAssertEqual(4, [testDataPacket dataLength]);
    
    [[NSFileManager defaultManager] removeItemAtPath:filePath error:nil];
}

@end
