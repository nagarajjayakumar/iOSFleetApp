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

#ifndef NiFiSiteToSiteConfig_h
#define NiFiSiteToSiteConfig_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 *
 * This contains extensions to the public / external configuration classes.
 */

#import <Foundation/Foundation.h>
#import "NiFiSiteToSite.h"

// MARK: - Proxy Configuration

@interface NiFiProxyConfig : NSObject <NSCopying>

@property (nonatomic, retain, readwrite, nonnull) NSURL *url;   // HTTP(S) URL of proxy, required
@property (nonatomic, retain, readwrite, nullable) NSString *username;  // optional proxy credentials for Basic Auth authenticaton
@property (nonatomic, retain, readwrite, nullable) NSString *password;  // optional proxy credentials for Basic Auth authenticaton

+ (nullable instancetype) proxyConfigWithUrl:(nonnull NSURL *)url;

@end


@interface NiFiSiteToSiteRemoteClusterConfig()

// proxyConfig allows one to specify an optional HTTP proxy to use to connect to remote cluster.
// Alternatively, proxy settings are configurable using the
// NiFiSiteToSiteRemoteClusterConfig.urlSessionConfiguration property,
// which is currently the recommended approach as it gives you more settings and options.
// For more information on using urlSessionConfiguration for proxy settings, see the README.md document.
@property (nonatomic, retain, readwrite, nullable) NiFiProxyConfig *proxyConfig;

@end


#endif /* NiFiSiteToSiteConfig_h */




