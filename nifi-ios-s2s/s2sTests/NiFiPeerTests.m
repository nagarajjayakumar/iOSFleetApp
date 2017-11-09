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

#import <XCTest/XCTest.h>
#import "NiFiSiteToSite.h"

@interface NiFiPeerTests : XCTestCase
@end


@implementation NiFiPeerTests

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testFactoryPeerWithUrl {
    NSURL *url = [NSURL URLWithString:@"https://example.com:8443"];
    NiFiPeer *peer = [NiFiPeer peerWithUrl:url];
    
    XCTAssertEqual(peer.url, url);
    XCTAssertEqual(peer.flowFileCount, 0);
    XCTAssertNil(peer.rawPort);
}

- (void)testFactoryPeerWithUrlAndPort {
    NSURL *url = [NSURL URLWithString:@"https://example.com:8443"];
    NiFiPeer *peer = [NiFiPeer peerWithUrl:url rawPort:@9090 rawIsSecure:true];
    
    XCTAssertEqual(peer.url, url);
    XCTAssertEqual(peer.flowFileCount, 0);
    XCTAssertEqual(peer.rawPort, @9090);
    XCTAssertEqual(peer.rawIsSecure, YES);
}

- (void)testMarkFailure {
    NSTimeInterval testStartTimestamp = [NSDate timeIntervalSinceReferenceDate];
    NSURL *url = [NSURL URLWithString:@"https://example.com:8443"];
    NiFiPeer *peer = [NiFiPeer peerWithUrl:url rawPort:@9090 rawIsSecure:true];
    [peer markFailure];
    
    XCTAssertGreaterThanOrEqual(peer.lastFailure, testStartTimestamp);
    XCTAssertLessThanOrEqual(peer.lastFailure, [NSDate timeIntervalSinceReferenceDate]);
}

- (void)testPeerKey {
    NSURL *url = [NSURL URLWithString:@"https://example.com:8443"];
    NiFiPeer *peer1 = [NiFiPeer peerWithUrl:url];
    NiFiPeer *peer2 = [NiFiPeer peerWithUrl:url];
    
    XCTAssertEqual(peer1.peerKey, peer2.peerKey);
    
    [peer2 markFailure];
    XCTAssertEqual(peer1.peerKey, peer2.peerKey);
    
    peer2.flowFileCount = 1;
    XCTAssertEqual(peer1.peerKey, peer2.peerKey);
}

- (void)testPeerCompareEqual {
    NSURL *url = [NSURL URLWithString:@"https://example.com:8443"];
    NiFiPeer *peer1 = [NiFiPeer peerWithUrl:url];
    NiFiPeer *peer2 = [NiFiPeer peerWithUrl:url];
    
    XCTAssertEqual(NSOrderedSame, [peer1 compare:peer2]);
}

- (void)testPeerCompareByFailure {
    NSURL *url = [NSURL URLWithString:@"https://example.com:8443"];
    NiFiPeer *peer1 = [NiFiPeer peerWithUrl:url];
    NiFiPeer *peer2 = [NiFiPeer peerWithUrl:url];
    
    peer1.lastFailure = 1.0; // should be first due to older failure time
    peer2.lastFailure = 2.0;
    
    XCTAssertEqual(NSOrderedAscending, [peer1 compare:peer2]);
    XCTAssertEqual(NSOrderedDescending, [peer2 compare:peer1]);
}

- (void)testPeerCompareByFlowFileCount {
    NSURL *url = [NSURL URLWithString:@"https://example.com:8443"];
    NiFiPeer *peer1 = [NiFiPeer peerWithUrl:url];
    NiFiPeer *peer2 = [NiFiPeer peerWithUrl:url];
    
    peer1.flowFileCount = 10; // should be first due to fewer flow files
    peer2.flowFileCount = 20;
    
    XCTAssertEqual(NSOrderedAscending, [peer1 compare:peer2]);
    XCTAssertEqual(NSOrderedDescending, [peer2 compare:peer1]);
}

- (void)testPeerCompareByHostname {
    NSURL *url1 = [NSURL URLWithString:@"https://a.example.com:8443"];
    NSURL *url2 = [NSURL URLWithString:@"https://b.example.com:8443"];
    NiFiPeer *peer1 = [NiFiPeer peerWithUrl:url1]; // should be first due to hostname
    NiFiPeer *peer2 = [NiFiPeer peerWithUrl:url2];
    
    XCTAssertEqual(NSOrderedAscending, [peer1 compare:peer2]);
    XCTAssertEqual(NSOrderedDescending, [peer2 compare:peer1]);
}

- (void)testPeerCompareByHttpPort {
    NSURL *url1 = [NSURL URLWithString:@"http://example.com:80"];
    NSURL *url2 = [NSURL URLWithString:@"http://example.com:81"];
    NiFiPeer *peer1 = [NiFiPeer peerWithUrl:url1]; // should be first due to http port
    NiFiPeer *peer2 = [NiFiPeer peerWithUrl:url2];
    
    XCTAssertEqual(NSOrderedAscending, [peer1 compare:peer2]);
    XCTAssertEqual(NSOrderedDescending, [peer2 compare:peer1]);
}

- (void)testPeerComparePrecedence {
    NSURL *url1 = [NSURL URLWithString:@"http://example.com:80"];
    NSURL *url2 = [NSURL URLWithString:@"http://example.com:81"];
    NiFiPeer *peer1 = [NiFiPeer peerWithUrl:url1];
    NiFiPeer *peer2 = [NiFiPeer peerWithUrl:url2];
    
    // Peer 1 is first based on port
    XCTAssertEqual(NSOrderedAscending, [peer1 compare:peer2]);
    XCTAssertEqual(NSOrderedDescending, [peer2 compare:peer1]);
    
    // Peer 2 takes precedence by hostname
    peer2.url = [NSURL URLWithString:@"http://a.example.com:81"];
    XCTAssertEqual(NSOrderedAscending, [peer2 compare:peer1]);
    XCTAssertEqual(NSOrderedDescending, [peer1 compare:peer2]);
    
    // Peer 1 takes precedence by Peer 2 having a higher flow file count
    peer2.flowFileCount = 10;
    XCTAssertEqual(NSOrderedAscending, [peer1 compare:peer2]);
    XCTAssertEqual(NSOrderedDescending, [peer2 compare:peer1]);
    
    // Peer 2 takes precedence by Peer 1 having a more recent failure
    [peer1 markFailure];
    XCTAssertEqual(NSOrderedAscending, [peer2 compare:peer1]);
    XCTAssertEqual(NSOrderedDescending, [peer1 compare:peer2]);
}

@end
