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

#ifndef NiFiSiteToSiteDatabase_h
#define NiFiSiteToSiteDatabase_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 */

#import <Foundation/Foundation.h>
#import "NiFiSiteToSiteService.h"

@interface NiFiQueuedDataPacketEntity : NSObject

@property (nonatomic, nullable) NSNumber *packetId;
@property (nonatomic, nullable) NSData *attributes;
@property (nonatomic, nullable) NSData *content;
@property (nonatomic, nullable) NSNumber *estimatedSize;
@property (nonatomic, nullable) NSNumber *createdAtMillisSinceReferenceDate;
@property (nonatomic, nullable) NSNumber *expiresAtMillisSinceReferenceDate;
@property (nonatomic, nullable) NSNumber *priority;
@property (nonatomic, nullable) NSString *transactionId;

+ (nullable instancetype)entityWithDataPacket:(nonnull NiFiDataPacket *)dataPacket
                            packetPrioritizer:(nullable NSObject <NiFiDataPacketPrioritizer> *)prioritizer
                                        error:(NSError *_Nullable *_Nullable)error;

- (nullable NiFiDataPacket *)dataPacket;

@end


typedef void (^NiFiQueuedDataPacketEntityEnumeratorBlock)(NiFiQueuedDataPacketEntity *_Nonnull packetEntity);


@interface NiFiSiteToSiteDatabase : NSObject

+ (nullable instancetype)sharedDatabase;

- (void)insertQueuedDataPacket:(nonnull NiFiQueuedDataPacketEntity *)entity error:(NSError *_Nullable *_Nullable)error;

- (void)insertQueuedDataPackets:(nonnull NSArray *)entities error:(NSError *_Nullable *_Nullable)error;

///* Enumerate each queued packet in priority order and call the caller's block function.
// * Continue until count limit or byte size limit is reached.
// * Pass '0' (or max long) for each limit to disable/ignore if you do no want to impose any limit, i.e. every packet will be enumerated.
// * Priority order is order by (priority, created, packetId) ascending */
//-(void)enumerateQueuedDataPacketsInPrioritizedOrder:(nonnull NiFiQueuedDataPacketEntityEnumeratorBlock)block
//                                         countLimit:(NSUInteger)countLimit  // pass 0 for no count limit
//                                      byteSizeLimit:(NSUInteger)sizeLimit   // pass 0 for no size limit
//                                updateEntitiesAtEnd:(BOOL)doUpdate;         // pass YES to update each entity after enumerating or NO if the block is read-only

-(void)createBatchWithTransactionId:(nonnull NSString *)transactionId
                         countLimit:(NSUInteger)countLimit                 // pass 0 for no count limit
                      byteSizeLimit:(NSUInteger)sizeLimit                  // pass 0 for no size limit
                              error:(NSError *_Nullable *_Nullable)error;

-(NSArray<NiFiQueuedDataPacketEntity *> *_Nullable)getPacketsWithTransactionId:(nonnull NSString *)transactionId;

-(void)deletePacketsWithTransactionId:(nonnull NSString *)transactionId;

-(void)markPacketsForRetryWithTransactionId:(nonnull NSString *)transactionId;

-(NSUInteger)countQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error;

-(NSUInteger)sumSizeQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error;

-(NSUInteger)averageSizeQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error;

/* Delete any packets where expiresAtMillisSinceReferenceDate > millisSinceReferenceDate */
-(void)ageOffExpiredQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error;

/* Keep a maximum number of data packets, ordered by priority.
 * Priority is order by (priority, created, packetId) ascending */
-(void)truncateQueuedDataPacketsMaxRows:(NSUInteger)maxRowsToKeepCount error:(NSError *_Nullable *_Nullable)error;

/* Keep a maximum number of data packets, ordered by priority.
 * Priority is order by (priority, created, packetId) ascending */
-(void)truncateQueuedDataPacketsMaxBytes:(NSUInteger)maxBytesToKeepSize error:(NSError *_Nullable *_Nullable)error;

@end


#endif /* NiFiSiteToSiteDatabase_h */
