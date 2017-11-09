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

#ifndef NiFiSiteToSiteClient_h
#define NiFiSiteToSiteClient_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 *
 * Specifically, this should only be imported by NiFiSiteToSiteClient.m and
 * test cases.
 */

#import <Foundation/Foundation.h>
#import "NiFiHttpRestApiClient.h"
#import "NiFiDataPacket.h"

@interface NiFiTransaction : NSObject <NiFiTransaction>

@property (nonatomic, retain, readwrite, nonnull) NSDate *startTime;
@property (nonatomic, readwrite) NiFiTransactionState transactionState;
@property (atomic, readwrite) bool shouldKeepAlive;
@property (nonatomic, readwrite, nonnull) NiFiDataPacketEncoder *dataPacketEncoder;
@property (nonatomic, readwrite, nullable) NiFiPeer *peer;

@end


@interface NiFiHttpTransaction : NiFiTransaction

@property (nonatomic, retain, readwrite, nonnull) NiFiHttpRestApiClient *restApiClient;
@property (nonatomic, readwrite, nonnull) NiFiTransactionResource *transactionResource;

- (nonnull instancetype) initWithPortId:(nonnull NSString *)portId
                      httpRestApiClient:(nonnull NiFiHttpRestApiClient *)restApiClient;
- (nonnull instancetype) initWithPortId:(nonnull NSString *)portId
                      httpRestApiClient:(nonnull NiFiHttpRestApiClient *)restApiClient
                                   peer:(nullable NiFiPeer *)peer;

@end


@interface NiFiSocketTransaction : NiFiTransaction

- (nonnull instancetype) initWithConfig:(nonnull NiFiSiteToSiteClientConfig *)config
                    remoteClusterConfig:(nonnull NiFiSiteToSiteRemoteClusterConfig *)remoteCluster
                                   peer:(nonnull NiFiPeer *)peer
                                 portId:(nonnull NSString *)portId;

@end

#endif /* NiFiSiteToSiteClient_h */
