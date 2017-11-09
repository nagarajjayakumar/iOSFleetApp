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
#import "NiFiSiteToSiteClient.h"

/********** Communicant/Peer Implementation **********/

@interface NiFiPeer()
@property (nonatomic, retain, readwrite, nonnull) NSURLComponents* urlComponents;
@property (nonatomic, readwrite) NSTimeInterval lastFailure;
@property (nonatomic, readwrite) NSUInteger flowFileCount;
@property (nonatomic, readwrite) bool secure;
@end


@implementation NiFiPeer

- initWithUrl:(nonnull NSURL *)url {
    return [self initWithUrl:url secure:false];
}

- initWithUrl:(nonnull NSURL *)url secure:(bool)isSecure {
    self = [super init];
    if(self != nil) {
        _urlComponents = [NSURLComponents componentsWithURL:url resolvingAgainstBaseURL:false];
        _secure = isSecure;
        _lastFailure = 0.0;
    }
    return self;
}

- (nullable NSURL *)url {
    return _urlComponents.URL;
}

- (nullable NSString *)host {
    return _urlComponents.host;
}

- (nullable NSNumber *)port {
    return _urlComponents.port;
}

- (void)markFailure {
    _lastFailure = [NSDate timeIntervalSinceReferenceDate];
}

- (NSComparisonResult)compare:(NiFiPeer *)other {
    NSInteger lastFailureMillis = _lastFailure * 1000;
    NSInteger otherlastFailureMillis = other.lastFailure + 1000;
    if (lastFailureMillis > otherlastFailureMillis) {
        return NSOrderedDescending;  // 1
    } else if (lastFailureMillis < otherlastFailureMillis) {
        return NSOrderedAscending;  // -1
    } else if (_flowFileCount > other.flowFileCount) {
        return NSOrderedDescending;
    } else if (_flowFileCount < other.flowFileCount) {
        return NSOrderedAscending;
    } else {
        if (_urlComponents.host) {
            if (other.urlComponents.host) {
                NSInteger hostCompare = [_urlComponents.host compare:other.urlComponents.host];
                if (hostCompare != NSOrderedSame) {
                    return hostCompare;
                }
            } else {
                return NSOrderedAscending;
            }
        }
        if (_urlComponents.port) {
            if (other.urlComponents.port) {
                NSInteger portCompare = [_urlComponents.port compare:other.urlComponents.port];
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
