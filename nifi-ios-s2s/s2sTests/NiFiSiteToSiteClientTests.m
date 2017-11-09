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

@interface NiFiSiteToSiteClientTests : XCTestCase
@end

@implementation NiFiSiteToSiteClientTests

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testSiteToSiteRemoteClusterConfig {
    NSURL *nifiUrl = [NSURL URLWithString:@"https://example.com:8080"];
    NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig = [NiFiSiteToSiteRemoteClusterConfig configWithUrl:nifiUrl];
    
    XCTAssertNotNil(remoteClusterConfig);
    XCTAssertNotNil(remoteClusterConfig.urls);
    XCTAssertEqual([remoteClusterConfig.urls count], 1);
    XCTAssertTrue([[remoteClusterConfig.urls anyObject] isEqual:nifiUrl]);
}

- (void)testSiteToSiteRemoteClusterConfigAddURL {
    NSURL *nifiUrl = [NSURL URLWithString:@"https://example.com:8080"];
    NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig = [NiFiSiteToSiteRemoteClusterConfig configWithUrl:nifiUrl];
    
    [remoteClusterConfig addUrl:nifiUrl]; // should have no effect as we are adding the same object to an NSSet.
    XCTAssertEqual([remoteClusterConfig.urls count], 1);
    
    NSURL *nifiUrl2 = [NSURL URLWithString:@"https://host2.example.com:8080"];
    [remoteClusterConfig addUrl:nifiUrl2]; // should have no effect as we are adding the same object to an NSSet.
    XCTAssertEqual([remoteClusterConfig.urls count], 2);
}

- (void)testSiteToSiteClientConfig {
    NSURL *nifiUrl = [NSURL URLWithString:@"https://example.com:8080"];
    NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig = [NiFiSiteToSiteRemoteClusterConfig configWithUrl:nifiUrl];
    NiFiSiteToSiteClientConfig *s2sConfig = [NiFiSiteToSiteClientConfig configWithRemoteCluster:remoteClusterConfig];
    
    XCTAssertNotNil(s2sConfig);
    XCTAssertNotNil(s2sConfig.remoteClusters);
    XCTAssertEqual([s2sConfig.remoteClusters count], 1);
    XCTAssertTrue([[s2sConfig.remoteClusters[0].urls anyObject] isEqual:nifiUrl]);
}

- (void)testSiteToSiteClientConfigAddCluster {
    NSURL *nifiUrl = [NSURL URLWithString:@"https://example.com:8080"];
    NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig = [NiFiSiteToSiteRemoteClusterConfig configWithUrl:nifiUrl];
    NiFiSiteToSiteClientConfig *s2sConfig = [NiFiSiteToSiteClientConfig configWithRemoteCluster:remoteClusterConfig];
    
    NSURL *nifiUrl2 = [NSURL URLWithString:@"https://host2.example.com:8080"];
    NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig2 = [NiFiSiteToSiteRemoteClusterConfig configWithUrl:nifiUrl2];
    [s2sConfig addRemoteCluster:remoteClusterConfig2];
    
    XCTAssertNotNil(s2sConfig);
    XCTAssertNotNil(s2sConfig.remoteClusters);
    XCTAssertEqual([s2sConfig.remoteClusters count], 2);
    XCTAssertTrue([[s2sConfig.remoteClusters[0].urls anyObject] isEqual:nifiUrl]);
    XCTAssertTrue([[s2sConfig.remoteClusters[1].urls anyObject] isEqual:nifiUrl2]);
}

- (void)testSiteToSiteClientFactory {
    NSURL *nifiUrl = [NSURL URLWithString:@"https://example.com:8080"];
    NiFiSiteToSiteRemoteClusterConfig *remoteClusterConfig = [NiFiSiteToSiteRemoteClusterConfig configWithUrl:nifiUrl];
    NiFiSiteToSiteClientConfig *s2sConfig = [NiFiSiteToSiteClientConfig configWithRemoteCluster:remoteClusterConfig];
    
    NiFiSiteToSiteClient *client = [NiFiSiteToSiteClient clientWithConfig:s2sConfig];
    XCTAssertNotNil(client);
}

@end
