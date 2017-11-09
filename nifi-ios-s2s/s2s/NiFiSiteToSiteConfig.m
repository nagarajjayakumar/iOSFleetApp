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

/********** ProxyConfig Implementation **********/

@implementation NiFiProxyConfig

+ (nullable instancetype) proxyConfigWithUrl:(nonnull NSURL *)url {
    id proxyConfig = nil;
    if (url) {
        proxyConfig = [[self alloc] initWithUrl:url];
    }
    return proxyConfig;
}

- (instancetype) initWithUrl:(NSURL *)url {
    self = [super init];
    if(self != nil) {
        _url = url;
        _username = nil;
        _password = nil;
    }
    return self;
}

- (id)copyWithZone:(NSZone *)zone {
    id copy = [[[self class] alloc] initWithUrl:[_url copyWithZone:zone]];
    ((NiFiProxyConfig *)copy).username = [_username copyWithZone:zone];
    ((NiFiProxyConfig *)copy).password = [_password copyWithZone:zone];
    return copy;
}

@end

/********** SiteToSiteRemoteClusterConfig Implementation **********/

@implementation NiFiSiteToSiteRemoteClusterConfig

+ (nullable instancetype) configWithUrl:(nonnull NSURL *)url {
    id remoteClusterConfig = [[self alloc] init];
    if (remoteClusterConfig && url) {
        [remoteClusterConfig addUrl:url];
    }
    return remoteClusterConfig;
}

+ (nullable instancetype) configWithUrls:(nonnull NSMutableSet<NSURL *> *)urls {
    id remoteClusterConfig = [[self alloc] init];
    if (remoteClusterConfig && urls) {
        ((NiFiSiteToSiteRemoteClusterConfig *)remoteClusterConfig).urls = [NSMutableSet setWithSet:urls];
    }
    return remoteClusterConfig;
}

- (instancetype) init {
    self = [super init];
    if(self != nil) {
        _urls = [[NSMutableSet<NSURL *> alloc] init];
        _transportProtocol = HTTP;
        _proxyConfig = nil;
        _username = nil;
        _password = nil;
        _urlSessionConfiguration = nil;
        _urlSessionDelegate = nil;
    }
    return self;
}

- (id)copyWithZone:(NSZone *)zone {
    id copy = [[[self class] alloc] init];
    
    for (NSURL *url in _urls) {
        [copy addUrl:[url copyWithZone:zone]];
    }
    ((NiFiSiteToSiteRemoteClusterConfig *)copy).transportProtocol = _transportProtocol;
    ((NiFiSiteToSiteRemoteClusterConfig *)copy).proxyConfig = _proxyConfig ? [_proxyConfig copyWithZone:zone] : nil;
    ((NiFiSiteToSiteRemoteClusterConfig *)copy).username = _username ? [_username copyWithZone:zone] : nil;
    ((NiFiSiteToSiteRemoteClusterConfig *)copy).password = _password ? [_password copyWithZone:zone] : nil;
    ((NiFiSiteToSiteRemoteClusterConfig *)copy).urlSessionConfiguration = _urlSessionConfiguration ? [_urlSessionConfiguration copyWithZone:zone] : nil;
    ((NiFiSiteToSiteRemoteClusterConfig *)copy).urlSessionDelegate = _urlSessionDelegate; // shallow copy

    return copy;
}

- (void) addUrl:(nonnull NSURL *)url {
    if (url) {
        if (!_urls) {
            _urls = [[NSMutableSet<NSURL *> alloc] init];
        }
        [_urls addObject:url];
    }
}

@end

/********** SiteToSiteClientConfig Implementation **********/

@implementation NiFiSiteToSiteClientConfig

+ (nullable instancetype) configWithRemoteCluster:(nonnull NiFiSiteToSiteRemoteClusterConfig *)remoteClusterConfig {
    id s2sConfig = [[self alloc] init];
    if (s2sConfig && remoteClusterConfig) {
        [s2sConfig addRemoteCluster:remoteClusterConfig];
    }
    return s2sConfig;
}

+ (nullable instancetype) configWithRemoteClusters:(nonnull NSArray<NiFiSiteToSiteRemoteClusterConfig *> *)remoteClusterConfigs {
    id s2sConfig = [[self alloc] init];
    if (s2sConfig && remoteClusterConfigs) {
        ((NiFiSiteToSiteClientConfig *)s2sConfig).remoteClusters = [NSMutableArray arrayWithArray:remoteClusterConfigs];
    }
    return s2sConfig;
}

- (instancetype) init {
    self = [super init];
    if(self != nil) {
        _remoteClusters = [[NSMutableArray<NiFiSiteToSiteRemoteClusterConfig *> alloc] init];
        _portName = nil;
        _portId = nil;
        _timeout = 30.0;
        _peerUpdateInterval = 0.0;
    }
    return self;
}

- (id)copyWithZone:(NSZone *)zone {
    
    id copy = [[[self class] alloc] init];
    
    for (NiFiSiteToSiteRemoteClusterConfig *cluster in _remoteClusters) {
        [copy addRemoteCluster:[cluster copyWithZone:zone]];
    }
    ((NiFiSiteToSiteClientConfig *)copy).portName = _portName ? [_portName copyWithZone:zone] : nil;
    ((NiFiSiteToSiteClientConfig *)copy).portId = _portId ? [_portId copyWithZone:zone] : nil;
    ((NiFiSiteToSiteClientConfig *)copy).timeout = _timeout;
    ((NiFiSiteToSiteClientConfig *)copy).peerUpdateInterval = _peerUpdateInterval;
    
    return copy;
}

- (void) addRemoteCluster:(nonnull NiFiSiteToSiteRemoteClusterConfig *)clusterConfig {
    if (clusterConfig) {
        if (!_remoteClusters) {
            _remoteClusters = [[NSMutableArray<NiFiSiteToSiteRemoteClusterConfig *> alloc] init];
        }
        [_remoteClusters addObject:clusterConfig];
    }
}

@end
