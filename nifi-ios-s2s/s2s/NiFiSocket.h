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

#ifndef NiFiSocket_h
#define NiFiSocket_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 *
 * This is just a light wrapper around NSStreams to make working with them a bit easier.
 */

#import <Foundation/Foundation.h>

/*! A socket with synchronous and asynchronous reads and writes that timeout
 *
 * When using the synchronous/blocking functions on this Socket type, you should not call them from the main / UI
 * thread. Call them from a background task / thread.
 **/
@interface NiFiSocket : NSObject

+ (nullable instancetype) socket;

// - (nullable instancetype) initWithAsyncSocket:(nonnull GCDAsyncSocket *)socket; // for testing only

- (BOOL) connectToHost:(nonnull NSString *)host onPort:(uint16_t)port error:(NSError *_Nullable *_Nullable)error;

- (void) startTLS:(nullable NSDictionary *)tlsSettings;

- (void) disconnect;

- (void) writeData:(nullable NSData *)data withTimeout:(NSTimeInterval)timeout error:(NSError *_Nullable *_Nullable)error;

- (void) writeData:(nullable NSData *)data withTimeout:(NSTimeInterval)timeout callback:(void (^_Nullable)(NSError *_Nullable))callback;

- (nullable NSData *) readDataToLength:(NSUInteger)length
                           withTimeout:(NSTimeInterval)timeout
                                 error:(NSError *_Nullable *_Nullable)error;

- (void) readDataToLength:(NSUInteger)length
              withTimeout:(NSTimeInterval)timeout
                 callback:(void (^_Nonnull)(NSData *_Nullable, NSError *_Nullable))callback;

- (nullable NSData *) readDataWithTimeout:(NSTimeInterval)timeout error:(NSError *_Nullable *_Nullable)error;

- (void) readDataWithTimeout:(NSTimeInterval)timeout callback:(void (^_Nonnull)(NSData *_Nullable, NSError *_Nullable))callback;

- (nullable NSData *) readDataAfterWriteData:(nonnull NSData*)data timeout:(NSTimeInterval)timeout error:(NSError *_Nullable *_Nullable)error;

@end

#endif /* NiFiSocket_h */
