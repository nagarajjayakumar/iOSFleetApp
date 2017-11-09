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
#import "NiFiSiteToSiteDatabaseFMDB.h"


@interface NiFiSiteToSiteDatabaseTests : XCTestCase
@property NiFiSiteToSiteDatabase *db;
@end

@implementation NiFiSiteToSiteDatabaseTests

- (void)setUp {
    [super setUp];
    // Put setup code here.
    _db = [[NiFiFMDBSiteToSiteDatabase alloc] initWithPersistenceType:PERSISTENT_TEMPORARY];
    // If you need to inspect the db manually, use the constructor below instead:
    //     NSString *testDbPath = [NSTemporaryDirectory() stringByAppendingPathComponent:@"nifi_sitetosite_test.db"];
    //     _db = [[NiFiFMDBSiteToSiteDatabase alloc] initWithDatabaseFilePath:testDbPath];
    // Then set a breakpoint in the test and look at the log for a line that has the file location of the sqlite database.
    // There is an example of this in testDatabaseMultiHandle
}

- (void)tearDown {
    // Put teardown code here.
    _db = nil; // will clear the underlying FMDB temporary database, which is deleted at dealloc
    [super tearDown];
}

- (void)testQueuedDataPacketEntity {
    
    NiFiDataPacket *packet = [NiFiDataPacket dataPacketWithAttributes:@{ @"key1": @"value1"}
                                                                 data:[@"Test Data" dataUsingEncoding:NSUTF8StringEncoding]];
    
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [[NiFiNoOpDataPacketPrioritizer alloc] init];
    NiFiQueuedDataPacketEntity *entity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet packetPrioritizer:prioritizer error:nil];
    
    XCTAssertNotNil(entity);
    XCTAssertNil(entity.packetId);
    XCTAssertNotNil(entity.attributes);
    XCTAssertTrue(entity.attributes.length > 0);
    XCTAssertNotNil(entity.content);
    XCTAssertTrue(entity.content.length > 0);
    XCTAssertNotNil(entity.estimatedSize);
    XCTAssertEqualWithAccuracy([entity.estimatedSize doubleValue], entity.attributes.length + entity.content.length, 5);
    XCTAssertNotNil(entity.createdAtMillisSinceReferenceDate);
    XCTAssertNotNil(entity.expiresAtMillisSinceReferenceDate);
    XCTAssertEqual([prioritizer ttlMillisForDataPacket:packet],
                   [entity.expiresAtMillisSinceReferenceDate integerValue] - [entity.createdAtMillisSinceReferenceDate integerValue]);
    XCTAssertNotNil(entity.priority);
    XCTAssertEqual(0, [entity.priority integerValue]);
    XCTAssertNil(entity.transactionId);
    
    NiFiDataPacket *packetFromEntity = [entity dataPacket];
    XCTAssertTrue([packetFromEntity.attributes isEqualToDictionary:packet.attributes]);
    XCTAssertTrue([packetFromEntity.data isEqualToData:packet.data]);
    
}

- (void)testDatabaseInsertPacket {
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizer];
    NiFiDataPacket *packet = [NiFiDataPacket dataPacketWithAttributes:@{ @"key1": @"value1"}
                                                                 data:[@"Test Data" dataUsingEncoding:NSUTF8StringEncoding]];
    NiFiQueuedDataPacketEntity *entity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet packetPrioritizer:prioritizer error:nil];
    
    [_db insertQueuedDataPacket:entity error:nil];
    
    XCTAssertEqual(1, [_db countQueuedDataPacketsOrError:nil]);
}

- (void)testDatabaseInsertPackets {
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizer];
    
    NiFiDataPacket *packet1 = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value1"}
                                                                 data:[@"Test Data 1" dataUsingEncoding:NSUTF8StringEncoding]];
    NiFiDataPacket *packet2 = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value2"}
                                                                  data:[@"Test Data 2" dataUsingEncoding:NSUTF8StringEncoding]];
    
    NiFiQueuedDataPacketEntity *entity1 = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet1 packetPrioritizer:prioritizer error:nil];
    NiFiQueuedDataPacketEntity *entity2 = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet2 packetPrioritizer:prioritizer error:nil];
    
    NSArray *entities = [NSArray arrayWithObjects:entity1, entity2, nil];
    
    [_db insertQueuedDataPackets:entities error:nil];
    
    XCTAssertEqual(2, [_db countQueuedDataPacketsOrError:nil]);
}

- (void)testDatabasePacketAgeOff {
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizerWithFixedTTL:0.6];
    NiFiDataPacket *packet = [NiFiDataPacket dataPacketWithAttributes:@{ @"key1": @"value1"}
                                                                 data:[@"Test Data" dataUsingEncoding:NSUTF8StringEncoding]];
    NiFiQueuedDataPacketEntity *entity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet packetPrioritizer:prioritizer error:nil];
    NSLog(@"Entity with expires=%ld", (long)[entity.expiresAtMillisSinceReferenceDate integerValue]);
    [_db insertQueuedDataPacket:entity error:nil];
    [_db ageOffExpiredQueuedDataPacketsOrError:nil]; // should have no affect when called immediately
    XCTAssertEqual(1, [_db countQueuedDataPacketsOrError:nil]);
    sleep(1); // sleep 1 second, so that the age-off period elapses
    [_db ageOffExpiredQueuedDataPacketsOrError:nil]; // should clear the queue
    XCTAssertEqual(0, [_db countQueuedDataPacketsOrError:nil]);
}

- (void)testDatabasePacketTruncateMaxRows {
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizer];
    
    NiFiDataPacket *packet1 = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value1"}
                                                                  data:[@"Test Data 1" dataUsingEncoding:NSUTF8StringEncoding]];
    NiFiDataPacket *packet2 = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value2"}
                                                                  data:[@"Test Data 2" dataUsingEncoding:NSUTF8StringEncoding]];
    
    NiFiQueuedDataPacketEntity *entity1 = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet1 packetPrioritizer:prioritizer error:nil];
    NiFiQueuedDataPacketEntity *entity2 = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet2 packetPrioritizer:prioritizer error:nil];
    
    NSArray *entities = [NSArray arrayWithObjects:entity1, entity2, nil];
    
    [_db insertQueuedDataPackets:entities error:nil];
    XCTAssertEqual(2, [_db countQueuedDataPacketsOrError:nil]);
    
    [_db truncateQueuedDataPacketsMaxRows:3 error:nil]; // should be no-op
    XCTAssertEqual(2, [_db countQueuedDataPacketsOrError:nil]);
    
    [_db truncateQueuedDataPacketsMaxRows:1 error:nil]; // should resize table to 1 row
    XCTAssertEqual(1, [_db countQueuedDataPacketsOrError:nil]);
}

- (void)testDatabasePacketTruncateMaxSize {
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizer];
    
    NiFiDataPacket *packet1 = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value1"}
                                                                  data:[@"Test Data 1" dataUsingEncoding:NSUTF8StringEncoding]];
    NiFiDataPacket *packet2 = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value2"}
                                                                  data:[@"Test Data 2" dataUsingEncoding:NSUTF8StringEncoding]];
    
    NiFiQueuedDataPacketEntity *entity1 = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet1 packetPrioritizer:prioritizer error:nil];
    NiFiQueuedDataPacketEntity *entity2 = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet2 packetPrioritizer:prioritizer error:nil];
    
    NSArray *entities = [NSArray arrayWithObjects:entity1, entity2, nil];
    
    [_db insertQueuedDataPackets:entities error:nil];
    XCTAssertEqual(2, [_db countQueuedDataPacketsOrError:nil]);
    
    [_db truncateQueuedDataPacketsMaxBytes:[entity1.estimatedSize integerValue] error:nil];
    
    [_db truncateQueuedDataPacketsMaxBytes:[entity1.estimatedSize integerValue] error:nil];
    
    XCTAssertEqual(1, [_db countQueuedDataPacketsOrError:nil]);
}

- (void)testDatabaseTransactionBatchingCount {
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizerWithFixedTTL:60.0];
    
    for (int i = 1; i <= 10; i++) {
        NiFiDataPacket *packet = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value"}
                                                                     data:[@"Test Data" dataUsingEncoding:NSUTF8StringEncoding]];
        NiFiQueuedDataPacketEntity *entity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet packetPrioritizer:prioritizer error:nil];
        
        [_db insertQueuedDataPacket:entity error:nil];
    }
    XCTAssertEqual(10, [_db countQueuedDataPacketsOrError:nil]);
    
    // 5 packets get assigned to transaction 1
    NSString *transactionId1 = @"12345678-1234-1234-1234-123456789abc";
    [_db createBatchWithTransactionId:transactionId1 countLimit:5 byteSizeLimit:0 error:nil];
    NSArray *transaction1Packets = [_db getPacketsWithTransactionId:transactionId1];
    XCTAssertEqual(5, [transaction1Packets count]);
    
    // all remaining unassigned packets get assigned to transaction 2 (it requests more than is left)
    NSString *transactionId2 = @"22345678-1234-1234-1234-123456789abd";
    [_db createBatchWithTransactionId:transactionId2 countLimit:10 byteSizeLimit:0 error:nil];
    NSArray *transaction2Packets = [_db getPacketsWithTransactionId:transactionId2];
    XCTAssertEqual(5, [transaction2Packets count]);
    
    // nothing left for transaction 3
    NSString *transactionId3 = @"32345678-1234-1234-1234-123456789abe";
    [_db createBatchWithTransactionId:transactionId3 countLimit:100 byteSizeLimit:0 error:nil];
    NSArray *transaction3Packets = [_db getPacketsWithTransactionId:transactionId3];
    XCTAssertEqual(0, [transaction3Packets count]);
    
    // transaction 1 succeeded
    [_db deletePacketsWithTransactionId:transactionId1];
    XCTAssertEqual(0, [[_db getPacketsWithTransactionId:transactionId1] count]);
    XCTAssertEqual(5, [_db countQueuedDataPacketsOrError:nil]);
    
    // transaction 2 failed, packets need retry
    [_db markPacketsForRetryWithTransactionId:transactionId2];
    XCTAssertEqual(0, [[_db getPacketsWithTransactionId:transactionId2] count]);
    XCTAssertEqual(5, [_db countQueuedDataPacketsOrError:nil]);
    
    // transaction 2's packets now available for transaction 4
    NSString *transactionId4 = @"42345678-1234-1234-1234-123456789abf";
    [_db createBatchWithTransactionId:transactionId4 countLimit:0 byteSizeLimit:0 error:nil];
    NSArray *transaction4Packets = [_db getPacketsWithTransactionId:transactionId4];
    XCTAssertEqual(5, [transaction4Packets count]);
    
    // transaction 4 succeeded
    [_db deletePacketsWithTransactionId:transactionId4];
    XCTAssertEqual(0, [[_db getPacketsWithTransactionId:transactionId4] count]);
    XCTAssertEqual(0, [_db countQueuedDataPacketsOrError:nil]);
}

- (void)testDatabaseTransactionBatchingSize {
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizerWithFixedTTL:60.0];
    
    NSInteger entitySize = 0;
    for (int i = 1; i <= 10; i++) {
        NiFiDataPacket *packet = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value"}
                                                                     data:[@"Test Data" dataUsingEncoding:NSUTF8StringEncoding]];
        NiFiQueuedDataPacketEntity *entity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet packetPrioritizer:prioritizer error:nil];
        entitySize = [entity.estimatedSize integerValue]; // all the same size
        
        [_db insertQueuedDataPacket:entity error:nil];
    }
    XCTAssertEqual(10, [_db countQueuedDataPacketsOrError:nil]);
    
    // 5 packets get assigned to transaction 1
    NSString *transactionId1 = @"12345678-1234-1234-1234-123456789abc";
    [_db createBatchWithTransactionId:transactionId1 countLimit:0 byteSizeLimit:5 * entitySize error:nil];
    NSArray *transaction1Packets = [_db getPacketsWithTransactionId:transactionId1];
    XCTAssertEqual(5, [transaction1Packets count]);
    
    // all remaining unassigned packets get assigned to transaction 2 (it requests more than is left)
    NSString *transactionId2 = @"22345678-1234-1234-1234-123456789abd";
    [_db createBatchWithTransactionId:transactionId2 countLimit:0 byteSizeLimit:10 * entitySize error:nil];
    NSArray *transaction2Packets = [_db getPacketsWithTransactionId:transactionId2];
    XCTAssertEqual(5, [transaction2Packets count]);
    
    // nothing left for transaction 3
    NSString *transactionId3 = @"32345678-1234-1234-1234-123456789abe";
    [_db createBatchWithTransactionId:transactionId3 countLimit:0 byteSizeLimit:INT_MAX error:nil];
    NSArray *transaction3Packets = [_db getPacketsWithTransactionId:transactionId3];
    XCTAssertEqual(0, [transaction3Packets count]);
    
    // transaction 1 succeeded
    [_db deletePacketsWithTransactionId:transactionId1];
    XCTAssertEqual(0, [[_db getPacketsWithTransactionId:transactionId1] count]);
    XCTAssertEqual(5, [_db countQueuedDataPacketsOrError:nil]);
    
    // transaction 2 failed, packets need retry
    [_db markPacketsForRetryWithTransactionId:transactionId2];
    XCTAssertEqual(0, [[_db getPacketsWithTransactionId:transactionId2] count]);
    XCTAssertEqual(5, [_db countQueuedDataPacketsOrError:nil]);
    
    // transaction 2's packets now available for transaction 4
    NSString *transactionId4 = @"42345678-1234-1234-1234-123456789abf";
    [_db createBatchWithTransactionId:transactionId4 countLimit:0 byteSizeLimit:0 error:nil];
    NSArray *transaction4Packets = [_db getPacketsWithTransactionId:transactionId4];
    XCTAssertEqual(5, [transaction4Packets count]);
    
    // transaction 4 succeeded
    [_db deletePacketsWithTransactionId:transactionId4];
    XCTAssertEqual(0, [[_db getPacketsWithTransactionId:transactionId4] count]);
    XCTAssertEqual(0, [_db countQueuedDataPacketsOrError:nil]);
}

- (void)testDatabaseLargeTransaction {
    int largePacketCount = 10000; // purposefully set to something much larger than NiFiFMDBSiteToSiteDatabase's DATABASE_BATCH_SIZE
    
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizerWithFixedTTL:120.0];
    
    for (int i = 1; i <= largePacketCount; i++) {
        NiFiDataPacket *packet = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value"}
                                                                     data:[@"Test Data" dataUsingEncoding:NSUTF8StringEncoding]];
        NiFiQueuedDataPacketEntity *entity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet packetPrioritizer:prioritizer error:nil];
        
        [_db insertQueuedDataPacket:entity error:nil];
    }
    XCTAssertEqual(largePacketCount, [_db countQueuedDataPacketsOrError:nil]);
    
    // 5 packets get assigned to transaction 1
    NSString *transactionId1 = @"12345678-1234-1234-1234-123456789abc";
    [_db createBatchWithTransactionId:transactionId1 countLimit:0 byteSizeLimit:0 error:nil];
    NSArray *transaction1Packets = [_db getPacketsWithTransactionId:transactionId1];
    XCTAssertEqual(largePacketCount, [transaction1Packets count]);
}
    

- (void)testDatabaseMultiHandle {
    NSString *testDbPath = [NSTemporaryDirectory() stringByAppendingPathComponent:@"nifi_sitetosite_test.db"];
    
    NSObject <NiFiDataPacketPrioritizer> *prioritizer = [NiFiNoOpDataPacketPrioritizer prioritizerWithFixedTTL:60.0];
    NiFiSiteToSiteDatabase *db1 = [[NiFiFMDBSiteToSiteDatabase alloc] initWithDatabaseFilePath:testDbPath];
    NiFiSiteToSiteDatabase *db2 = [[NiFiFMDBSiteToSiteDatabase alloc] initWithDatabaseFilePath:testDbPath];
    
    for (int i = 1; i <= 10; i++) {
        NiFiDataPacket *packet = [NiFiDataPacket dataPacketWithAttributes:@{ @"key": @"value"}
                                                                     data:[@"Test Data" dataUsingEncoding:NSUTF8StringEncoding]];
        NiFiQueuedDataPacketEntity *entity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet packetPrioritizer:prioritizer error:nil];
        [db1 insertQueuedDataPacket:entity error:nil];
        [db2 insertQueuedDataPacket:entity error:nil];
    }
    XCTAssertEqual(20, [db1 countQueuedDataPacketsOrError:nil]);
    XCTAssertEqual(20, [db2 countQueuedDataPacketsOrError:nil]);
    
    // all packets get assigned to transaction 1
    NSString *transactionId1 = @"12345678-1234-1234-1234-123456789abc";
    [db1 createBatchWithTransactionId:transactionId1 countLimit:0 byteSizeLimit:0 error:nil];
    NSArray *transaction1Packets = [db2 getPacketsWithTransactionId:transactionId1];
    XCTAssertEqual(20, [transaction1Packets count]);
    
    db1 = nil;
    db2 = nil;

    NiFiSiteToSiteDatabase *db3 = [[NiFiFMDBSiteToSiteDatabase alloc] initWithDatabaseFilePath:testDbPath];
    XCTAssertEqual(20, [db3 countQueuedDataPacketsOrError:nil]);
    [db3 deletePacketsWithTransactionId:transactionId1];
    XCTAssertEqual(0, [db3 countQueuedDataPacketsOrError:nil]);
    
    [[NSFileManager defaultManager] removeItemAtPath:testDbPath error:nil];
}



@end
