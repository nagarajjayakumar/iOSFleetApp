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

/********** Peer/Communicant Implementation **********/

@implementation NiFiPeer

+ (nullable instancetype)peerWithUrl:(NSURL *)url {
    return [self peerWithUrl:url rawPort:nil rawIsSecure:NO];
}

+ (nullable instancetype)peerWithUrl:(NSURL *)url rawPort:(nullable NSNumber *)rawPort rawIsSecure:(BOOL)secure {
    return [[self alloc] initWithUrl:url rawPort:rawPort rawIsSecure:secure];
}

- initWithUrl:(nonnull NSURL *)url rawPort:(nullable NSNumber *)rawPort rawIsSecure:(BOOL)secure {
    self = [super init];
    if (self) {
        _url = url;
        _rawPort = rawPort;
        _rawIsSecure = secure;
        _flowFileCount = 0;
        _lastFailure = 0.0;
    }
    return self;
}

- (void)markFailure {
    _lastFailure = [NSDate timeIntervalSinceReferenceDate];
}

- (id)peerKey {
    // flowFileCount and lastFailure are not part of the key.
    // currently, the key is just the url, made absolute because we always want to treat resolved locations as equal.
    return [_url absoluteURL];
}

- (NSComparisonResult)compare:(NiFiPeer *)other {
    NSInteger lastFailureMillis = _lastFailure * 1000;
    NSInteger otherlastFailureMillis = other.lastFailure * 1000;
    if (lastFailureMillis > otherlastFailureMillis) {
        return NSOrderedDescending;  // 1
    } else if (lastFailureMillis < otherlastFailureMillis) {
        return NSOrderedAscending;  // -1
    } else if (_flowFileCount > other.flowFileCount) {
        return NSOrderedDescending;
    } else if (_flowFileCount < other.flowFileCount) {
        return NSOrderedAscending;
    } else {
        if (_url.host) {
            if (other.url.host) {
                NSInteger hostCompare = [_url.host compare:other.url.host];
                if (hostCompare != NSOrderedSame) {
                    return hostCompare;
                }
            } else {
                return NSOrderedAscending;
            }
        }
        if (_url.port) {
            if (other.url.port) {
                NSInteger portCompare = [_url.port compare:other.url.port];
                if (portCompare != NSOrderedSame) {
                    return portCompare;
                }
            } else {
                return NSOrderedAscending;
            }
        }
    }
    return NSOrderedSame;
}

@end
