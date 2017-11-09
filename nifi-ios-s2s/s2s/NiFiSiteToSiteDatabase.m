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
#import "fmdb/FMDB.h"
#import "NiFiError.h"
#import "NiFiSiteToSiteService.h"
#import "NiFiSiteToSiteDatabaseFMDB.h"

// Reasonably sized batches for bulk DB operations
static const NSUInteger DATABASE_BATCH_SIZE = 2000L;

/********** QueuedDataPacketEntity Implementation **********/

@implementation NiFiQueuedDataPacketEntity

+ (instancetype)entityWithDataPacket:(nonnull NiFiDataPacket *)dataPacket
                   packetPrioritizer:(nullable NSObject <NiFiDataPacketPrioritizer> *)prioritizer
                               error:(NSError *_Nullable *_Nullable)error {
    
    if (!dataPacket) {
        return nil;
    }
    
    if (!prioritizer) {
        prioritizer = [[NiFiNoOpDataPacketPrioritizer alloc] init];
    }
    
    NiFiQueuedDataPacketEntity *entity = [[self alloc] init];
    entity.packetId = nil; // will be set on insert
    
    NSError *serializationError = nil;
    NSData *serializedAttributes = [NSJSONSerialization dataWithJSONObject:dataPacket.attributes options:0 error:&serializationError];
    if (!serializationError && serializedAttributes) {
        entity.attributes = serializedAttributes;
    } else {
        if (error && serializationError) {
            NSLog(@"Error serializing data packet attributes. %@", serializationError.localizedDescription);
            *error = serializationError;
        }
    }
    if (!dataPacket.data) {
        entity.content = nil;
        entity.estimatedSize = [NSNumber numberWithUnsignedLong:entity.attributes.length];
    } else {
        entity.content = [NSData dataWithData:dataPacket.data];
        entity.estimatedSize = [NSNumber numberWithUnsignedLong:(entity.attributes.length + entity.content.length)];
    }
    NSUInteger createdAtMillisSinceReferenceDate = [NSDate timeIntervalSinceReferenceDate] * 1000L;
    entity.createdAtMillisSinceReferenceDate = [NSNumber numberWithLong:createdAtMillisSinceReferenceDate];
    NSUInteger expiresAtMillisSinceReferenceDate = createdAtMillisSinceReferenceDate  + [prioritizer ttlMillisForDataPacket:dataPacket];
    entity.expiresAtMillisSinceReferenceDate = [NSNumber numberWithLong:expiresAtMillisSinceReferenceDate];
    entity.priority = [NSNumber numberWithInteger:[prioritizer priorityForDataPacket:dataPacket]];
    entity.transactionId = nil;
    return entity;
}

- (instancetype)init {
    self = [super init];
    if (self) {
        // no additional initialization
    }
    return self;
}

- (nullable NiFiDataPacket *)dataPacket {
    NSError *jsonDecodingError;
    NSDictionary *attributes = _attributes ? [NSJSONSerialization JSONObjectWithData:_attributes
                                                                             options:0
                                                                               error:&jsonDecodingError]: [NSDictionary dictionary];
    if (jsonDecodingError) {
        NSLog(@"Unexpected error decoding data packet from database. Did the database format change without existing records getting updated?");
        return nil;
    }
    NiFiDataPacket *dataPacket = [NiFiDataPacket dataPacketWithAttributes:attributes data:_content];
    return dataPacket;
}

@end


/********** SiteToSiteDatabase Implementation **********/

/* The abstract base class and interface to the NiFiSiteToSiteDatabase class cluster
 * The only method it implements is obtaining the singleton sharedDatabase instance,
 * which currently is an instance of the the FMDB concrete class implementation.
 */
@implementation NiFiSiteToSiteDatabase

+ (instancetype)sharedDatabase {
    static NiFiSiteToSiteDatabase *_sharedDatabase = nil;
    static dispatch_once_t oncePredicate;
    dispatch_once(&oncePredicate, ^{
        _sharedDatabase = [[NiFiFMDBSiteToSiteDatabase alloc] init];
    });
    return _sharedDatabase;
}

- (void)insertQueuedDataPacket:(NiFiQueuedDataPacketEntity *)entity error:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];}

- (void)insertQueuedDataPackets:(NSArray *)entities error:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];}

-(void)createBatchWithTransactionId:(nonnull NSString *)transactionId
                         countLimit:(NSUInteger)countLimit
                      byteSizeLimit:(NSUInteger)sizeLimit
                              error:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(NSArray<NiFiQueuedDataPacketEntity *> *_Nullable)getPacketsWithTransactionId:(nonnull NSString *)transactionId {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(void)deletePacketsWithTransactionId:(nonnull NSString *)transactionId {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(void)markPacketsForRetryWithTransactionId:(nonnull NSString *)transactionId {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(NSUInteger)countQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(NSUInteger)sumSizeQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(NSUInteger)averageSizeQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(void)ageOffExpiredQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(void)truncateQueuedDataPacketsMaxRows:(NSUInteger)maxRowsToKeepCount error:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

-(void)truncateQueuedDataPacketsMaxBytes:(NSUInteger)maxBytesToKeepSize error:(NSError *_Nullable *_Nullable)error {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"You must override %@ in a subclass", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

@end




/********** SiteToSiteDatabase FMDB-based Implementation **********/

@interface FMResultSet (FMResultSetAdditions)
- (id)objectOrNilForColumn:(nonnull NSString *)column;
@end

@implementation FMResultSet (FMResultSetAdditions)
- (id)objectOrNilForColumn:(nonnull NSString *)column {
    id object = [self objectForColumn:column];
    return object == [NSNull null] ? nil : object;
}
@end


static NSString * const NIFI_SITETOSITE_DB_FILE_LOCATION = @"nifi_sitetosite.db";


@interface NiFiFMDBSiteToSiteDatabase()
@property (atomic) FMDatabaseQueue *fmdbQueue;
@end


@implementation NiFiFMDBSiteToSiteDatabase

- (nullable instancetype)init {
    return [self initWithPersistenceType:PERSISTENT_DEFAULT];
}


- (nullable instancetype)initWithPersistenceType:(FMDBPersistenceType)persistenceType {
    NSString *databaseFilePath = [[self class] databaseFilePathFromPersistenceType:persistenceType];
    return [self initWithDatabaseFilePath:databaseFilePath];
}

- (nullable instancetype)initWithDatabaseFilePath:(NSString *)path {
    self = [super init];
    if (self) {
        // if db file does not exist, it will get created (i.e., on first launch)
        // _fmdb = [FMDatabase databaseWithPath:[self databaseFilePath]];
        _fmdbQueue = [FMDatabaseQueue databaseQueueWithPath:path];
        
        if (![self createOrUpdateSchema]) {
            self = nil;
        }
    }
    return self;
}

+ (NSString *)databaseFilePathFromPersistenceType:(FMDBPersistenceType)persistenceType {
    // For how this works with FMDB, see https://github.com/ccgus/fmdb/blob/master/README.markdown#database-creation
    switch (persistenceType) {
        case VOLATILE_IN_MEMORY:
            return NULL;
        case PERSISTENT_TEMPORARY:
            return @"";
        case PERSISTENT_DEFAULT:
        default: {
            NSString *s2sFrameworkBundlePath = [[NSBundle bundleWithIdentifier:@"org.apache.nifi.s2s"] bundlePath];
            NSString *databaseFilePath = [s2sFrameworkBundlePath stringByAppendingPathComponent:NIFI_SITETOSITE_DB_FILE_LOCATION];
            return databaseFilePath;
        }
    }
}

- (bool)createOrUpdateSchema {
    // Schema v1
    NSMutableArray *schemaUpdates = [NSMutableArray array];
    [schemaUpdates addObject:
     @"CREATE TABLE IF NOT EXISTS site_to_site_queued_packet ("
        "packet_id INTEGER PRIMARY KEY, "  // auto assigned primary key
        "attributes BLOB, "                // attributes of data packet, encoded as JSON string
        "content BLOB, "                   // byte array holding content of data packet
        "estimated_size INTEGER, "          // estimated size of the data packet in bytes were it to be sent to a NiFi peer"
        "created INTEGER, "                // timestamp of data packet creation in form of milliseconds since reference date
        "expires INTEGER, "                // timestamp of data packet expiration in form of milliseconds since reference date
        "priority INTEGER, "               // priority (lower value is higher priority)
        "transaction_id CHAR(36) )"        // UUID of transaction generated as part of site-to-site protocol
     
    ];
    [schemaUpdates addObjectsFromArray:@[
     @"CREATE INDEX IF NOT EXISTS site_to_site_queued_packet_transaction_id_index ON site_to_site_queued_packet (transaction_id)",
     @"CREATE INDEX IF NOT EXISTS site_to_site_queued_packet_expires_index ON site_to_site_queued_packet (expires)",
     @"CREATE INDEX IF NOT EXISTS site_to_site_queued_packet_sort_index ON site_to_site_queued_packet (priority, created, packet_id)",
     ]];
    
    // TO UPDATE THIS SCHEMA:
    //  - Add additional schema migration lines below without altering the above.
    //  - Don't alter the above "CREATE TABLE" statements. Instead, use "ALTER TABLE" statements
    //    Example:
    //      ALTER TABLE
    //  - The idea is that if you go through the schema versions in order you should be able to
    //    get from any version to any later version.
    //  - In order to maintain backwards compatibility between adjacent versions, don't:
    //      - add non-nullable columns
    //      - rename columns
    //      - change types
    //      - etc.
    //
    // For example, you could add something along the lines of:
    //
    // Schema vNEXT
    // [schemaUpdates addObjectsFromArray:@[@"ALTER TABLE ADD COLUMN ..."]]
    
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        // Log output that is useful for development / testing to find the location of the DB in use in case you want to inspect that directly
        NSString *databasePath = [db databasePath] ?: @"nil";
        NSLog(@"Path to SiteToSite SQLite Database: '%@'", databasePath);
        
        for (NSString *update in schemaUpdates) {
            [db executeUpdate:update];
        }
    }];
    return true;
}

- (void)insertQueuedDataPacket:(NiFiQueuedDataPacketEntity *)entity error:(NSError *_Nullable *_Nullable)error {
    NSArray *entities = [NSArray arrayWithObject:entity];
    return [self insertQueuedDataPackets:entities error:error];
}

- (void)insertQueuedDataPackets:(NSArray *)entities error:(NSError *_Nullable *_Nullable)error {
    
    __block BOOL success;
    
    [_fmdbQueue inTransaction:^(FMDatabase * _Nonnull db, BOOL * _Nonnull rollback) {
        for (NiFiQueuedDataPacketEntity *entity in entities) {
            success = [db executeUpdate:@"INSERT INTO site_to_site_queued_packet "
                       "(attributes, content, estimated_size, created, expires, priority, transaction_id)"
                       "VALUES (?, ?, ?, ?, ?, ?, ?)",
                       entity.attributes ?: [NSNull null],
                       entity.content ?: [NSNull null],
                       entity.estimatedSize ?: [NSNull null],
                       entity.createdAtMillisSinceReferenceDate ?: [NSNull null],
                       entity.expiresAtMillisSinceReferenceDate ?: [NSNull null],
                       entity.priority ?: [NSNull null],
                       entity.transactionId ?: [NSNull null]
                       ];
            
            if (!success) {
                *rollback = YES;
                return;
            }
        }
    }];
    
    if (!success && error) {
        *error = [NSError errorWithDomain:NiFiErrorDomain
                                     code:NiFiErrorSiteToSiteDatabaseTransactionFailed
                                 userInfo:nil];
    }
}

///* Enumerate each queued packet in priority order and call the caller's block function.
// * Continue until count limit or byte size limit is reached.
// * Pass '0' (or max long) for each limit to disable/ignore if you do no want to impose any limit, i.e. every packet will be enumerated.
// * Priority order is order by (priority, created, packetId) ascending */
//-(void)enumerateQueuedDataPacketsInPrioritizedOrder:(NiFiQueuedDataPacketEntityEnumeratorBlock)block
//                                         countLimit:(NSUInteger)countLimit
//                                      byteSizeLimit:(NSUInteger)sizeLimit
//                                updateEntitiesAtEnd:(BOOL)doUpdate {
//    __block BOOL success;
//    
//    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
//        FMResultSet *resultSet = [db executeQuery:@"SELECT * FROM site_to_site_queued_packet ORDER BY "
//                                  "priority, created, packet_id ASC "
//                                  "LIMIT ?", countLimit];
//        success = (resultSet != nil);
//        if (!success) {
//            return;
//        }
//        
//        NSUInteger batchSize = 0;
//        while ([resultSet next]) {
//            NiFiQueuedDataPacketEntity *entity = [[self class] queuedDataPacketEntityWithFMResult:resultSet];
//            if (!entity) {
//                continue;
//            }
//            batchSize += [entity.estimatedSize unsignedIntegerValue];
//            block(entity);
//            if (batchSize >= sizeLimit) {
//                break;
//            }
//        }
//    }];
//    
//    if (success) {
//        
//    }
//}

-(void)createBatchWithTransactionId:(nonnull NSString *)transactionId
                         countLimit:(NSUInteger)countLimit
                      byteSizeLimit:(NSUInteger)sizeLimit
                              error:(NSError *_Nullable *_Nullable)error {
    __block NSError *blockError;
    
    [_fmdbQueue inTransaction:^(FMDatabase *_Nonnull db, BOOL *_Nonnull rollback) {
        FMResultSet *resultSet;
        if (countLimit) {
            resultSet = [db executeQuery:@"SELECT * FROM site_to_site_queued_packet "
                                            "WHERE transaction_id IS NULL "
                                            "ORDER BY priority, created, packet_id ASC "
                                            "LIMIT ?", [NSNumber numberWithLong:countLimit]];
        } else {
            resultSet = [db executeQuery:@"SELECT * FROM site_to_site_queued_packet "
                                            "WHERE transaction_id IS NULL "
                                            "ORDER BY priority, created, packet_id ASC "];
        }
        if (resultSet == nil) {
            blockError = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorSiteToSiteDatabaseReadFailed userInfo:nil];
            return;
        }
        
        NSMutableArray *transactionPackets = countLimit ? [NSMutableArray arrayWithCapacity:countLimit] : [NSMutableArray arrayWithCapacity:DATABASE_BATCH_SIZE];
        NSUInteger batchSize = 0;
        while ([resultSet next]) {
            NiFiQueuedDataPacketEntity *entity = [[self class] queuedDataPacketEntityWithFMResult:resultSet];
            if (!entity || !entity.packetId) {
                NSLog(@"Unexpected error converting FMResultSet to NiFiQueuedDataPacketEntity in %@", NSStringFromSelector(_cmd));
                continue;
            }
            
            // put each packet id in an array for a batch update below that will set the transaction id on all of them.
            [transactionPackets addObject:entity.packetId];
            
            if (sizeLimit) {
                batchSize += [entity.estimatedSize unsignedIntegerValue];
                if (batchSize >= sizeLimit) {
                    break;
                }
            }
        }
        [resultSet close]; // explicit close recommended here due to break statement in while loop
        
        NSMutableArray *updateBatches = [NSMutableArray array];
        NSUInteger itemsRemaining = [transactionPackets count];
        int i = 0;
        while (itemsRemaining) {
            NSRange range = NSMakeRange(i, MIN(DATABASE_BATCH_SIZE, itemsRemaining));
            NSMutableArray *subarray = [NSMutableArray arrayWithArray:[transactionPackets subarrayWithRange:range]];
            [updateBatches addObject:subarray];
            itemsRemaining -= range.length;
            i += range.length;
        }
        for (NSMutableArray *updateBatch in updateBatches) {
            NSMutableArray *placeholders = [NSMutableArray arrayWithCapacity:[updateBatch count] + 1];
            for (int i=0; i<[updateBatch count]; i++) {
                [placeholders addObject:@"?"];
            }
            NSString *placeholderString = [placeholders componentsJoinedByString:@", "];
            
            NSString *updateStatement = [NSString stringWithFormat:
                                         @"UPDATE site_to_site_queued_packet SET transaction_id = ? WHERE packet_id IN (%@)", placeholderString];
            
            [updateBatch insertObject:transactionId atIndex:0]; // this is the value for the 'SET transaction_id = ?' part of the update statement
            
            Boolean success = [db executeUpdate:updateStatement withArgumentsInArray:updateBatch];
            if (!success) {
                *rollback = YES; // something went wrong. rollback the marked packets so that they get picked up in a future transaction
                blockError = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorSiteToSiteDatabaseWriteFailed userInfo:nil];
                return;
            }
        }
    }];
    
    if (blockError && error) {
        *error = blockError;
    }
}

-(NSArray<NiFiQueuedDataPacketEntity *> *_Nullable)getPacketsWithTransactionId:(nonnull NSString *)transactionId {
    __block NSMutableArray<NiFiQueuedDataPacketEntity *> *transactionPackets = nil;
    
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        FMResultSet *resultSet = [db executeQuery:@"SELECT * FROM site_to_site_queued_packet WHERE transaction_id = ? "
                                  "ORDER BY priority, created, packet_id ASC ", transactionId];
        if (resultSet == nil) {
            return;
        }
        
        transactionPackets = [NSMutableArray array];
        while ([resultSet next]) {
            NiFiQueuedDataPacketEntity *entity = [[self class] queuedDataPacketEntityWithFMResult:resultSet];
            if (!entity) {
                NSLog(@"Unexpected error converting FMResultSet to NiFiQueuedDataPacketEntity in %@", NSStringFromSelector(_cmd));
                continue;
            }
            [transactionPackets addObject:entity];
        }
    }];
    
    return transactionPackets;
}

-(void)deletePacketsWithTransactionId:(nonnull NSString *)transactionId {
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        [db executeUpdate:@"DELETE FROM site_to_site_queued_packet WHERE transaction_id = ?", transactionId];
    }];
}

-(void)markPacketsForRetryWithTransactionId:(NSString *)transactionId {
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        [db executeUpdate:@"UPDATE site_to_site_queued_packet SET transaction_id = NULL WHERE transaction_id = ?", transactionId];
    }];
}



-(NSUInteger)countQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    
    __block BOOL success;
    __block NSInteger rowCount;
    
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        FMResultSet *resultSet = [db executeQuery:@"SELECT COUNT(*) as count FROM site_to_site_queued_packet"];
        success = (resultSet != nil);
        
        if (success && [resultSet next]) {
            rowCount = [resultSet longForColumn:@"count"];
        }
        [resultSet close];
    }];
    
    if (!success && error) {
        *error = [NSError errorWithDomain:NiFiErrorDomain
                                     code:NiFiErrorSiteToSiteDatabaseReadFailed
                                 userInfo:nil];
    }
    
    return rowCount;
}

-(NSUInteger)sumSizeQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    
    __block BOOL success;
    __block NSInteger size;
    
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        FMResultSet *resultSet = [db executeQuery:@"SELECT sum(estimated_size) as total_size FROM site_to_site_queued_packet"];
        success = (resultSet != nil);
        
        if (success && [resultSet next]) {
            size = [resultSet longForColumn:@"total_size"];
        }
        [resultSet close];
    }];
    
    if (!success && error) {
        *error = [NSError errorWithDomain:NiFiErrorDomain
                                     code:NiFiErrorSiteToSiteDatabaseReadFailed
                                 userInfo:nil];
    }
    
    return size;
}

-(NSUInteger)averageSizeQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    
    __block BOOL success;
    __block NSInteger size;
    
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        FMResultSet *resultSet = [db executeQuery:@"SELECT avg(estimated_size) as average_size FROM site_to_site_queued_packet"];
        success = (resultSet != nil);
        
        if (success && [resultSet next]) {
            size = [resultSet longForColumn:@"average_size"];
        }
        [resultSet close];
    }];
    
    if (!success && error) {
        *error = [NSError errorWithDomain:NiFiErrorDomain
                                     code:NiFiErrorSiteToSiteDatabaseReadFailed
                                 userInfo:nil];
    }
    
    return size;
}

/* Delete any packets where expiresAtMillisSinceReferenceDate > millisSinceReferenceDate */
-(void)ageOffExpiredQueuedDataPacketsOrError:(NSError *_Nullable *_Nullable)error {
    
    __block BOOL success;
    
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        NSNumber *nowMillis = [NSNumber  numberWithLong:([NSDate timeIntervalSinceReferenceDate] * 1000.0)];
        success = [db executeUpdate:@"DELETE FROM site_to_site_queued_packet WHERE expires < ?", nowMillis];
    }];
    
    if (!success && error) {
        *error = [NSError errorWithDomain:NiFiErrorDomain
                                     code:NiFiErrorSiteToSiteDatabaseWriteFailed
                                 userInfo:nil];
    }

}

/* Keep a maximum number of data packets, ordered by priority.
 * Priority is order by (priority, created, packetId) ascending */
-(void)truncateQueuedDataPacketsMaxRows:(NSUInteger)maxRowsToKeepCount error:(NSError *_Nullable *_Nullable)error {
    
    __block BOOL success;
    
    [_fmdbQueue inDatabase:^(FMDatabase * _Nonnull db) {
        NSNumber *rowsToKeepCount = [NSNumber numberWithLong:maxRowsToKeepCount];
        success = [db executeUpdate:@"DELETE FROM site_to_site_queued_packet "
                                        "WHERE packet_id NOT IN ( "
                                        "SELECT packet_id FROM site_to_site_queued_packet "
                                        "ORDER BY priority, created, packet_id ASC "
                                        "LIMIT ? )", rowsToKeepCount];
    }];
    
    if (!success && error) {
        *error = [NSError errorWithDomain:NiFiErrorDomain
                                     code:NiFiErrorSiteToSiteDatabaseWriteFailed
                                 userInfo:nil];
    }

}

/* Keep a maximum number of data packets, ordered by priority.
 * Priority is order by (priority, created, packetId) ascending */
-(void)truncateQueuedDataPacketsMaxBytes:(NSUInteger)maxBytesToKeepSize error:(NSError *_Nullable *_Nullable)error {
    
    __block Boolean success;
    __block NSError *blockError = nil;
    
    [_fmdbQueue inTransaction:^(FMDatabase * _Nonnull db, BOOL * _Nonnull rollback) {
        
        // Check if the queue size exceeds maxBytesToKeepSize
        FMResultSet *resultSet = [db executeQuery:@"SELECT sum(estimated_size) as total_size FROM site_to_site_queued_packet"];
        if (resultSet != nil && [resultSet next]) {
            NSInteger totalByteSize = [resultSet longForColumn:@"total_size"];
            if (totalByteSize <= maxBytesToKeepSize) {
                success = TRUE;
                [resultSet close];
                return;
            }
        } else {
            blockError = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorSiteToSiteDatabaseReadFailed userInfo:nil];
            [resultSet close];
            return;
        }
        [resultSet close];
        
        NSMutableArray *deleteBatches = [NSMutableArray arrayWithCapacity:1L];
        NSInteger currentBatchIndex = 0;
        deleteBatches[currentBatchIndex] = [NSMutableSet setWithCapacity:DATABASE_BATCH_SIZE];
        NSUInteger sizeAggregator = 0;
        
        resultSet = [db executeQuery:@"SELECT * FROM site_to_site_queued_packet "
                                        "ORDER BY priority, created, packet_id ASC"];
        success = (resultSet != nil);
        if (!success) {
            blockError = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorSiteToSiteDatabaseReadFailed userInfo:nil];
            return;
        }
        Boolean haveReachedMaxCapacity = false;
        while ([resultSet next]) {
            NiFiQueuedDataPacketEntity *entity = [[self class] queuedDataPacketEntityWithFMResult:resultSet];
            if (!entity || !entity.packetId) {
                NSLog(@"Unexpected entity read error in %@", NSStringFromSelector(_cmd));
                continue;
            }
            if (!haveReachedMaxCapacity) {
                sizeAggregator += [entity.estimatedSize unsignedIntegerValue];
                haveReachedMaxCapacity = sizeAggregator >= maxBytesToKeepSize;
            } else {
                [deleteBatches[currentBatchIndex] addObject:[entity.packetId stringValue]];
                if ( [deleteBatches[currentBatchIndex] count] >= DATABASE_BATCH_SIZE ) {
                    currentBatchIndex++;
                    deleteBatches[currentBatchIndex] = [NSMutableSet setWithCapacity:DATABASE_BATCH_SIZE];
                }
            }
            
        }
        [resultSet close];
        for (NSMutableSet *delBatch in deleteBatches) {
            NSInteger count = [delBatch count];
            if (count > 0) {
                NSMutableString *bindPlaceholderBuilder = [NSMutableString string];
                for (int i = 0; i < [delBatch count]; i++) {
                    [bindPlaceholderBuilder appendString:@"?, "];
                }
                NSString *bindPlaceholder = [bindPlaceholderBuilder substringToIndex:[bindPlaceholderBuilder length]-2]; // remove the final ", "
                [db executeUpdate:[NSString stringWithFormat:@"DELETE FROM site_to_site_queued_packet WHERE packet_id in (%@)", bindPlaceholder]
                           values:[delBatch allObjects]
                            error:&blockError];
            }
        }
    }];
     
    if ((!success || blockError) && error) {
        *error = blockError;
    }
}

+ (NiFiQueuedDataPacketEntity *)queuedDataPacketEntityWithFMResult:(FMResultSet *)result {
    
    NiFiQueuedDataPacketEntity *entity = [[NiFiQueuedDataPacketEntity alloc] init];
    entity.packetId = [result objectOrNilForColumn:@"packet_id"];
    entity.attributes = [result objectOrNilForColumn:@"attributes"];
    entity.content = [result objectOrNilForColumn:@"content"];
    entity.estimatedSize = [result objectOrNilForColumn:@"estimated_size"];
    entity.createdAtMillisSinceReferenceDate = [result objectOrNilForColumn:@"created"];
    entity.expiresAtMillisSinceReferenceDate = [result objectOrNilForColumn:@"expires"];
    entity.priority = [result objectOrNilForColumn:@"priority"];
    entity.transactionId = [result objectOrNilForColumn:@"transaction_id"];
    
    return entity;
    
}

-(void)dealloc {
    if (_fmdbQueue) {
        _fmdbQueue = nil;
    }
}

@end

