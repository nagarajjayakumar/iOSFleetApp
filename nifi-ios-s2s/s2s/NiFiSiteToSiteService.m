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
#import "NiFiSiteToSiteService.h"
#import "NiFiSiteToSiteClient.h"
#import "NiFiSiteToSiteDatabase.h"
#import "NiFiError.h"

// static const int SECONDS_TO_NANOS = 1000000000;

/********** No Op DataPacketPrioritizer Implementation **********/

@interface NiFiNoOpDataPacketPrioritizer()
@property (nonatomic) NSInteger fixedTtlMillis;
@end

@implementation NiFiNoOpDataPacketPrioritizer

+ (nonnull instancetype)prioritizer {
    return [self prioritizerWithFixedTTL:1.0];
}

+ (nonnull instancetype)prioritizerWithFixedTTL:(NSTimeInterval)ttl {
    return [[self alloc] initWithFixedTTL:ttl];
}

- initWithFixedTTL:(NSTimeInterval)ttl {
    self = [super init];
    if (self) {
        _fixedTtlMillis = (NSInteger)(ttl * 1000.0); // convert NSTimeInterval to millis
    }
    return self;
}

- (NSInteger)priorityForDataPacket:(nonnull NiFiDataPacket *)dataPacket {
    return 0;
}

- (NSInteger)ttlMillisForDataPacket:(nonnull NiFiDataPacket *)dataPacket {
    return _fixedTtlMillis;
}

@end


/********** QueuedSiteToSiteConfig Implementation **********/

static const int QUEUED_S2S_CONFIG_DEFAULT_MAX_PACKET_COUNT = 10000L;
static const int QUEUED_S2S_CONFIG_DEFAULT_MAX_PACKET_SIZE = 100L * 1024L * 1024L; // 100 MB
static const int QUEUED_S2S_CONFIG_DEFAULT_BATCH_COUNT = 100L;
static const int QUEUED_S2S_CONFIG_DEFAULT_BATCH_SIZE = 1024L * 1024L; // 1 MB

@implementation NiFiQueuedSiteToSiteClientConfig

-(instancetype)init {
    self = [super init];
    if (self) {
        _maxQueuedPacketCount = [NSNumber numberWithInteger:QUEUED_S2S_CONFIG_DEFAULT_MAX_PACKET_COUNT];
        _maxQueuedPacketSize = [NSNumber numberWithInteger:QUEUED_S2S_CONFIG_DEFAULT_MAX_PACKET_SIZE];
        _preferredBatchCount = [NSNumber numberWithInteger:QUEUED_S2S_CONFIG_DEFAULT_BATCH_COUNT];
        _preferredBatchSize = [NSNumber numberWithInteger:QUEUED_S2S_CONFIG_DEFAULT_BATCH_SIZE];
        _dataPacketPrioritizer = [[NiFiNoOpDataPacketPrioritizer alloc] init];
    }
    return self;
}

@end


/********** SiteToSiteQueueStatus Implementation **********/

@interface NiFiSiteToSiteQueueStatus()
@property (nonatomic, readwrite) NSUInteger queuedPacketCount;
@property (nonatomic, readwrite) NSUInteger queuedPacketSizeBytes;
@property (nonatomic, readwrite) BOOL isFull;
@end

@implementation NiFiSiteToSiteQueueStatus : NSObject
@end


/********** QueuedSiteToSiteClient Implementation **********/

@interface NiFiQueuedSiteToSiteClient()

@property NiFiQueuedSiteToSiteClientConfig *config;
@property NiFiSiteToSiteDatabase *database;

@end


@implementation NiFiQueuedSiteToSiteClient

+ (nonnull instancetype)clientWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config {
    return [[self alloc] initWithConfig:config];
}

- (instancetype)initWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config {
    return [self initWithConfig:config
                       database:[NiFiSiteToSiteDatabase sharedDatabase]];
}

- (nullable instancetype)initWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
                               database:(nonnull NiFiSiteToSiteDatabase *)database
{
    self = [super init];
    if (self != nil) {
        _config = config;
        _database = database;
    }
    return self;
}

- (void) enqueueDataPacket:(nonnull NiFiDataPacket *)dataPacket error:(NSError *_Nullable *_Nullable)error {
    [self enqueueDataPackets:[NSArray arrayWithObjects:dataPacket, nil] error:error];
}

- (void) enqueueDataPackets:(nonnull NSArray *)dataPackets error:(NSError *_Nullable *_Nullable)error {
    
    if ([dataPackets count] <= 0) {
        return;
    }
    
    NSMutableArray *entitiesToInsert = [[NSMutableArray alloc] initWithCapacity:[dataPackets count]];
    for (NiFiDataPacket *packet in dataPackets) {
        NSError *entityConversionError = nil;
        NiFiQueuedDataPacketEntity *queuedPacketEntity = [NiFiQueuedDataPacketEntity entityWithDataPacket:packet
                                                                                        packetPrioritizer:_config.dataPacketPrioritizer
                                                                                                    error:&entityConversionError];
        if (entityConversionError) {
            NSLog(@"Error enqueing data packet to local buffer database. %@", entityConversionError.localizedDescription);
            if (*error) {
                *error = entityConversionError;
            }
            return;
        }
        [entitiesToInsert addObject:queuedPacketEntity];
    }
    [_database insertQueuedDataPackets:entitiesToInsert error:error];
}

- (void) processOrError:(NSError *_Nullable *_Nullable)error {
    
    // Check for work to do (non-zero queued packet count)
    NSError *dbError;
    NSUInteger queuedPacketCount = [_database countQueuedDataPacketsOrError:&dbError];
    if (!dbError && queuedPacketCount == 0) {
        return;
    }
    
    // create a site-to-site client and initiate a trasaction with the nifi peer
    // we need the server-generated transaction id to continue with the db operation
    NiFiSiteToSiteClient *client = [NiFiSiteToSiteClient clientWithConfig:_config];
    id transaction = [client createTransaction];
    if (!transaction || ![transaction transactionId]) {
        if (error) {
            *error = [NSError errorWithDomain:NiFiErrorDomain
                                         code:NiFiErrorSiteToSiteClientCouldNotCreateTransaction
                                     userInfo:nil];
        }
        return;
    }
    NSString *transactionId = [transaction transactionId];
    
    // use the server-generated transaction id to mark packets for transmission
    [_database createBatchWithTransactionId:transactionId
                                 countLimit:[_config.preferredBatchCount unsignedIntegerValue]
                              byteSizeLimit:[_config.preferredBatchSize unsignedIntegerValue]
                                      error:&dbError];
    
    if (dbError) {
        NSLog(@"Encountered error with domain='%@' code='%ld", [*error domain], (long)[*error code]);
        if (error) {
            *error = dbError;
        }
        return;
    }
    
    // now send the data to the nifi peer in a transaction
    NSError *transactionError;
    NSArray<NiFiQueuedDataPacketEntity *> *entitiesToSend = [_database getPacketsWithTransactionId:transactionId];
    if ([entitiesToSend count] > 0) {
        for (NiFiQueuedDataPacketEntity *entity in entitiesToSend) {
            [transaction sendData:[entity dataPacket]];
        }
        [transaction confirmAndCompleteOrError:&transactionError];
    } else {
        // nothing to do, perhaps another task/thread cleared the queue
        [transaction cancel];
    }
    
    // if the transaction completed, remove the queued packets from the DB, otherwise, mark them for retry. 
    if (transactionError) {
        NSLog(@"Encountered error with domain='%@' code='%ld", [*error domain], (long)[*error code]);
        if (error) {
            *error = transactionError;
        }
        [_database markPacketsForRetryWithTransactionId:transactionId];
        return;
    } else {
        // successfully sent data packets; clear them from the queue
        [_database deletePacketsWithTransactionId:transactionId];
    }
}

- (void) cleanupOrError:(NSError *_Nullable *_Nullable)error {
    
    // delete expired packets
    [_database ageOffExpiredQueuedDataPacketsOrError:error];
    
    // delete lowest priority packets over row count limit
    NSInteger maxCount = _config.maxQueuedPacketCount ? [_config.maxQueuedPacketCount integerValue] : 0;
    [_database truncateQueuedDataPacketsMaxRows:maxCount error:error];
    
    // delete lowest priority packets over the packet byte size limit
    NSInteger maxBytes = _config.maxQueuedPacketSize ? [_config.maxQueuedPacketSize integerValue] : 0;
    [_database truncateQueuedDataPacketsMaxBytes:maxBytes error:error];
}

- (nullable NiFiSiteToSiteQueueStatus *) queueStatusOrError:(NSError *_Nullable *_Nullable)error {
    NiFiSiteToSiteQueueStatus *status = [[NiFiSiteToSiteQueueStatus alloc] init];
    NSError *dbError = nil;
    
    status.queuedPacketCount = [_database countQueuedDataPacketsOrError:&dbError];
    if (dbError) {
        if (error) {
            *error = dbError;
        }
        return nil;
    }
    
    status.queuedPacketSizeBytes = [_database sumSizeQueuedDataPacketsOrError:&dbError];
    if (dbError) {
        if (error) {
            *error = dbError;
        }
        return nil;
    }
    
    status.isFull = FALSE;
    if (self.config.maxQueuedPacketCount && [self.config.maxQueuedPacketCount integerValue]) {
        status.isFull = status.queuedPacketCount >= [self.config.maxQueuedPacketCount integerValue] ? YES : NO;
    }
    if(!status.isFull) {
        if (self.config.maxQueuedPacketSize && [self.config.maxQueuedPacketSize integerValue]) {
            NSUInteger averageSize = [_database averageSizeQueuedDataPacketsOrError:&dbError];
            if (!dbError) {
                status.isFull =
                    status.queuedPacketSizeBytes >= [self.config.maxQueuedPacketSize integerValue] - averageSize ?
                    YES : NO;
            } else {
                if (error) {
                    *error = dbError;
                }
            }
        }
    }
    return status;
}

@end


/********** SiteToSiteService Implementation **********/

@implementation NiFiSiteToSiteService

+ (void)sendDataPacket:(nonnull NiFiDataPacket *)packet
                config:(nonnull NiFiSiteToSiteClientConfig *)config
     completionHandler:(void (^_Nullable)(NiFiTransactionResult *_Nullable result, NSError *_Nullable error))completionHandler {
    NSArray *packets = [NSArray arrayWithObjects:packet, nil];
    [[self class] sendDataPackets:packets
                           config:config
                completionHandler:completionHandler];
}

+ (void)sendDataPackets:(nonnull NSArray *)packets
                 config:(nonnull NiFiSiteToSiteClientConfig *)config
      completionHandler:(void (^_Nullable)(NiFiTransactionResult *_Nullable result, NSError *_Nullable error))completionHandler {
    
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        NiFiTransactionResult *result = nil;
        NSError *error = nil;
        
        NiFiSiteToSiteClient *s2sClient = [NiFiSiteToSiteClient clientWithConfig:config];
        id transaction = [s2sClient createTransaction];
        if (transaction) {
            for (NiFiDataPacket *packet in packets) {
                [transaction sendData:packet];
            }
            result = [transaction confirmAndCompleteOrError:&error];
        } else {
            error = [NSError errorWithDomain:NiFiErrorDomain
                                        code:NiFiErrorSiteToSiteClientCouldNotCreateTransaction
                                    userInfo:@{NSLocalizedDescriptionKey: @"Could not create site-to-site transaction. Check configuration and remote cluster reachability."}];
        }
        
        completionHandler(result, error);
    });
}

+ (void)enqueueDataPacket:(nonnull NiFiDataPacket *)packet
                   config:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
        completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                             NSError *_Nullable error))completionHandler {
    
    NSArray *packets = [NSArray arrayWithObjects:packet, nil];
    
    return [[self class] enqueueDataPackets:packets
                                     config:config
                          completionHandler:completionHandler];
}

+ (void)enqueueDataPackets:(nonnull NSArray *)packets
                    config:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
         completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                              NSError *_Nullable error))completionHandler {
    
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0), ^{
        NiFiSiteToSiteQueueStatus *status = nil;
        NSError *error = nil;
        NiFiQueuedSiteToSiteClient *s2sClient = [NiFiQueuedSiteToSiteClient clientWithConfig:config];
        [s2sClient enqueueDataPackets:packets error:&error];
        if (!error) {
            [s2sClient cleanupOrError:&error];
            if (!error) {
                status = [s2sClient queueStatusOrError:&error];
            }
        }
        completionHandler(status, error);
    });
}

+ (void)processQueuedPacketsWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
                     completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                                          NSError *_Nullable error))completionHandler {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_DEFAULT, 0), ^{
        NiFiSiteToSiteQueueStatus *status = nil;
        NSError *error = nil;
        NiFiQueuedSiteToSiteClient *s2sClient = [NiFiQueuedSiteToSiteClient clientWithConfig:config];
        [s2sClient cleanupOrError:&error];
        if (!error) {
            [s2sClient processOrError:&error];
            if (!error) {
                status = [s2sClient queueStatusOrError:&error];
            }
        }
        completionHandler(status, error);
    });
}

+ (void)cleanupQueuedPacketsWithConfig:(nonnull NiFiQueuedSiteToSiteClientConfig *)config
           completionHandler:(void (^_Nullable)(NiFiSiteToSiteQueueStatus *_Nullable status,
                                                NSError *_Nullable error))completionHandler {
    dispatch_async(dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_LOW, 0), ^{
        NiFiSiteToSiteQueueStatus *status = nil;
        NSError *error = nil;
        NiFiQueuedSiteToSiteClient *s2sClient = [NiFiQueuedSiteToSiteClient clientWithConfig:config];
        [s2sClient cleanupOrError:&error];
        if (!error) {
            status = [s2sClient queueStatusOrError:&error];
        }
        completionHandler(status, error);
    });
}

@end


