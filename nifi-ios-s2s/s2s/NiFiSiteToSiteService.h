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

#ifndef NiFiSiteToSiteService_h
#define NiFiSiteToSiteService_h

/* Visibility: External / Public
 *
 * This header defines a public interface of the s2s framework / module.
 */

#import <Foundation/Foundation.h>
#import "NiFiSiteToSite.h"


/* This protocol can be implemented by the user of this framework to specify app-specific buisiness logic for
 * prioritizing and aging-off data packets when using the asynchronous, queued NiFiSiteToSiteService interface.
 * You can implement your own class that adheres to this protocol and pass it into the interface for queuing 
 * packets to send. */
@protocol NiFiDataPacketPrioritizer <NSObject>

/* Returns the priority of the Data Packet, e.g., based on the data packets attributes or contents.
 * Lower values will recieve highest priority, i.e., '0' is higher priority than '1', which is higher priority than '10', etc. */
- (NSInteger)priorityForDataPacket:(nonnull NiFiDataPacket *)dataPacket;

/* Returns the TTL in milliseconds for the data packet. 
 * This TTL value is interpreted asrelative to the time the packet is created/queued. */
- (NSInteger)ttlMillisForDataPacket:(nonnull NiFiDataPacket *)dataPacket;

@end


/* An example implementation of a NiFiDataPacketPrioritizer. The implementation can be found in NiFiSiteToSiteService.m
 * This always returns priority = 0 (i.e., all packets will be treated as the same priority)
 * This always returns a TTL that is specified in the factory method (defaults to 1 second) */
@interface NiFiNoOpDataPacketPrioritizer : NSObject <NiFiDataPacketPrioritizer>

// Construct a NoOpPrioritizer that always responds priority = 0, TTL = 1s
+(nonnull instancetype)prioritizer;

// Construct a NoOpPrioritizer that always responds = 0, TTL = ttl
+(nonnull instancetype)prioritizerWithFixedTTL:(NSTimeInterval)ttl;
@end


@interface NiFiQueuedSiteToSiteClientConfig : NiFiSiteToSiteClientConfig <NSCopying>
@property (nonatomic, retain, readwrite, nonnull)NSNumber *maxQueuedPacketCount; // defaults to 10000 data packets
@property (nonatomic, retain, readwrite, nonnull)NSNumber *maxQueuedPacketSize;  // defaults to 100 MB
@property (nonatomic, retain, readwrite, nonnull)NSNumber *preferredBatchCount;  // defaults to 100 data packets
@property (nonatomic, retain, readwrite, nonnull)NSNumber *preferredBatchSize;   // defaults to 1 MB
@property (nonatomic, retain, readwrite, nonnull)NSObject <NiFiDataPacketPrioritizer> *dataPacketPrioritizer; // defaults to NiFiNoOpDataPacketPrioritizer
@end


@interface NiFiSiteToSiteQueueStatus : NSObject

@property (nonatomic, readonly) NSUInteger queuedPacketCount;
@property (nonatomic, readonly) NSUInteger queuedPacketSizeBytes;
@property (nonatomic, readonly) BOOL isFull;

@end


@interface NiFiQueuedSiteToSiteClient : NSObject

+ (nonnull instancetype)clientWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config;

- (void) enqueueDataPacket:(nonnull NiFiDataPacket *)dataPacket error:(NSError *_Nullable *_Nullable)error;
- (void) enqueueDataPackets:(nonnull NSArray *)dataPackets error:(NSError *_Nullable *_Nullable)error;
- (void) processOrError:(NSError *_Nullable *_Nullable)error;
- (void) cleanupOrError:(NSError *_Nullable *_Nullable)error;
- (nullable NiFiSiteToSiteQueueStatus *) queueStatusOrError:(NSError *_Nullable *_Nullable)error;

@end


@interface NiFiSiteToSiteService : NSObject

+ (void)sendDataPacket:(nonnull NiFiDataPacket *)packet
                config:(nonnull NiFiSiteToSiteClientConfig *)config
     completionHandler:(void (^_Nullable)(NiFiTransactionResult *_Nullable result, NSError *_Nullable error))completionHandler;

+ (void)sendDataPackets:(nonnull NSArray *)packets
                 config:(nonnull NiFiSiteToSiteClientConfig *)config
      completionHandler:(void (^_Nullable)(NiFiTransactionResult *_Nullable result, NSError *_Nullable error))completionHandler;

+ (void)enqueueDataPacket:(nonnull NiFiDataPacket *)packet
                   config:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
        completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                             NSError *_Nullable error))completionHandler;

+ (void)enqueueDataPackets:(nonnull NSArray *)packets
                    config:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
         completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                              NSError *_Nullable error))completionHandler;

+ (void)processQueuedPacketsWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
                     completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                                          NSError *_Nullable error))completionHandler;

+ (void)cleanupQueuedPacketsWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
                     completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                                          NSError *_Nullable error))completionHandler;

@end


#endif /* NiFiSiteToSiteService_h */
