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

#ifndef NiFiHttpRestApiClient_h
#define NiFiHttpRestApiClient_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 */

#import <Foundation/Foundation.h>
#import "NiFiSiteToSiteUtil.h"
#import "NiFiSiteToSiteTransaction.h"
#import "NiFiDataPacket.h"

// A protocol for Apple's NSURLSession, a dependecy of NiFiHttpRestApiClient
// This protocol is used to inject stubs/mocks in testing.
@protocol NSURLSessionProtocol <NSObject>
- (NSURLSessionDataTask *_Null_unspecified)dataTaskWithRequest:(NSURLRequest *_Null_unspecified)request
                                             completionHandler:(void (^_Null_unspecified)(NSData *_Nullable data, NSURLResponse *_Nullable response, NSError *_Nullable error))completionHandler;
@end


@interface NiFiTransactionResource : NSObject
- (nonnull instancetype)initWithTransactionId:(nonnull NSString *)transactionId;
@property (nonatomic, assign, readwrite, nonnull) NSString *transactionId;
@property (nonatomic, assign, readwrite, nullable) NSString *transactionUrl;
@property (nonatomic, readwrite) NSInteger serverSideTtl;
@property (nonatomic, readwrite) NSUInteger flowFilesSent;
@property (nonatomic, readwrite) NiFiTransactionResponseCode lastResponseCode;
@property (nonatomic, assign, readwrite, nullable) NSString *lastResponseMessage;
@end


@interface NiFiHttpRestApiClient : NSObject

- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl;

- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl
                        clientCredential:(nullable NSURLCredential *)credendtial;

- (nonnull instancetype) initWithBaseUrl:(nonnull NSURL *)baseUrl
                        clientCredential:(nullable NSURLCredential *)credendtial
                              urlSession:(nonnull NSObject<NSURLSessionProtocol> *)urlSession;

- (nullable NSURL *)baseUrl;

- (nullable NSDictionary *)getSiteToSiteInfoOrError:(NSError *_Nullable *_Nullable)error;

- (nullable NSDictionary *)getRemoteInputPortsOrError:(NSError *_Nullable *_Nullable)error;

- (nullable NSArray<NiFiPeer *> *)getPeersOrError:(NSError *_Nullable *_Nullable)error;

- (nullable NiFiTransactionResource *)initiateSendTransactionToPortId:(nonnull NSString *)portId
                                                                error:(NSError *_Nullable *_Nullable)error;

- (void)extendTTLForTransaction:(nonnull NSString *)transactionUrl error:(NSError *_Nullable *_Nullable)error;

- (NSInteger)sendFlowFiles:(nonnull NiFiDataPacketEncoder *)dataPacketEncoder
           withTransaction:(nonnull NiFiTransactionResource *)transactionResource
                     error:(NSError *_Nullable *_Nullable)error; // also returns -1 if an error occured

- (nullable NiFiTransactionResult *)endTransaction:(nonnull NSString *)transactionUrl
                                      responseCode:(NiFiTransactionResponseCode)responseCode
                                             error:(NSError *_Nullable *_Nullable)error;

@end

#endif /* NiFiHttpRestApiClient_h */
