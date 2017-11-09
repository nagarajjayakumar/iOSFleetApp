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

@import CocoaAsyncSocket;
# import "NiFiSocket.h"
# import "NiFiError.h"

@interface Tag : NSObject
+ (nonnull instancetype) tagWithLongValue:(long)value;
+ (NSString *) keyForTagLongValue:(long)value;
@property NSString *key;
@property long longValue;
@end

@implementation Tag
+ (instancetype) tagWithLongValue:(long)value {
    Tag *tag = [[self alloc] init];
    tag.key = [self keyForTagLongValue:value];
    tag.longValue = value;
    return tag;
}

+ (NSString *) keyForTagLongValue:(long)value {
    return [NSString stringWithFormat:@"%li", value];
}
@end


@interface NiFiSocket() <GCDAsyncSocketDelegate>
@property (nonatomic) GCDAsyncSocket *socket;
@property (nonatomic) long nextTagValue;
@property (nonatomic) Tag *nextTag;
@property NSMutableDictionary<NSString *, void (^)(NSData *, NSError *)> *readCallbackForTag;
@property NSMutableDictionary<NSString *, void (^)(NSError *)> *writeCallbackForTag;
@end


@implementation NiFiSocket

+ (instancetype) socket {
    GCDAsyncSocket *socket = [[GCDAsyncSocket alloc] init];
    return [[self alloc] initWithAsyncSocket:socket] ;
}

- (instancetype) initWithAsyncSocket:(GCDAsyncSocket *)socket {
    self = [super init];
    if (self) {
        _nextTagValue = 0L;
        _readCallbackForTag = [NSMutableDictionary dictionary];
        _writeCallbackForTag = [NSMutableDictionary dictionary];
        _socket = socket;
        [_socket setDelegate:self];
        [_socket setDelegateQueue:dispatch_get_global_queue(DISPATCH_QUEUE_PRIORITY_HIGH, 0)];
    }
    return self;
}

-(void)dealloc {
    [self.socket setDelegate:nil];
    [self.socket disconnectAfterReadingAndWriting];
}

// MARK: Private functions

- (void) nextTagValue:(long)value {
    @throw [NSException
            exceptionWithName:NSInternalInconsistencyException
            reason:[NSString stringWithFormat:@"nextTagValue is read only. Cannot call %@", NSStringFromSelector(_cmd)]
            userInfo:nil];
}

- (long) nextTagValue {
    @synchronized(self) {
        if (_nextTagValue == LONG_MAX) {
            _nextTagValue = LONG_MIN;
        } else {
            _nextTagValue++;
        }
        return _nextTagValue;
    }
}

- (Tag *) uniqueTag {
    Tag *tag = [Tag tagWithLongValue:[self nextTagValue]];
    // assert Tag is not in use, which could only happen if we wrapped-around all long values.
    if ([self isTagInUse:tag]) {
        @throw [NSException
                exceptionWithName:NSInternalInconsistencyException
                reason:[NSString stringWithFormat:@"%@: nextTagValue has overflown. Cannot generate a unique tag.", NSStringFromSelector(_cmd)]
                userInfo:nil];
    }
    return tag;
}
        
- (BOOL) isTagInUse:(Tag *)tag {
    return ([self.readCallbackForTag objectForKey:tag.key] != nil);
}

- (void) destroyTag:(Tag *)tag {
    [self.readCallbackForTag removeObjectForKey:tag.key];
}

// MARK: GCDAsyncSocket Wrapper Functions

- (BOOL) connectToHost:(nonnull NSString *)host onPort:(uint16_t)port error:(NSError *_Nullable *_Nullable)error {
    NSError *socketError;
    BOOL success = [_socket connectToHost:host onPort:port error:&socketError]; // The actaul connection is asynchronous.
    if (!success) {
        NSLog(@"Could not connect to host: %@", socketError);
        if (error) {
            *error = socketError;
        }
    }
    return success;
}

- (void)startTLS:(NSDictionary *)tlsSettings {
    [self.socket startTLS:tlsSettings];
}

- (void) disconnect {
    [self.socket disconnectAfterReadingAndWriting];
    self.socket.delegate = nil;
}

- (void) writeData:(nullable NSData *)data withTimeout:(NSTimeInterval)timeout callback:(void (^_Nullable)(NSError *_Nullable))callback {
    Tag *tag = [self uniqueTag];
    [self.writeCallbackForTag setValue:callback forKey:tag.key];
    [self.socket writeData:data withTimeout:timeout tag:tag.longValue];
    // The callback will be invoked from the didWriteData:tag: GCDAsyncSocketDelegate function
}

- (void) writeData:(nullable NSData *)data withTimeout:(NSTimeInterval)timeout error:(NSError *_Nullable *_Nullable)error {
    __block BOOL callbackExecuted = NO;
    __block NSError *outerError = nil;
    
    [self writeData:data withTimeout:timeout callback:^(NSError *_Nullable error) {
        outerError = error;
        callbackExecuted = YES;
    }];
    
    while (!callbackExecuted) {
        [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode beforeDate:[NSDate distantFuture]];
    }
    
    if (error && outerError) {
        *error = outerError;
    }
}

- (void) readDataWithTimeout:(NSTimeInterval)timeout callback:(void (^)(NSData *, NSError *))callback {
    // store callback by tag for later
    Tag *tag = [self uniqueTag];
    [self.readCallbackForTag setValue:callback forKey:tag.key];
    [self.socket readDataWithTimeout:timeout tag:tag.longValue];
}

- (NSData *) readDataWithTimeout:(NSTimeInterval)timeout error:(NSError **)error {
    __block BOOL callbackExecuted = NO;
    __block NSData *outerData = nil;
    __block NSError *outerError = nil;
    
    [self readDataWithTimeout:timeout callback:^(NSData *data, NSError *error) {
        outerData = data;
        outerError = error;
        callbackExecuted = YES;
    }];
    
    while (!callbackExecuted) {
        [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode beforeDate:[NSDate distantFuture]];
    }
    
    if (error && outerError) {
        *error = outerError;
    }
    
    return outerData;
}

- (void) readDataToLength:(NSUInteger)length withTimeout:(NSTimeInterval)timeout callback:(void (^)(NSData *, NSError *))callback {
    // store callback by tag for later
    Tag *tag = [self uniqueTag];
    [self.readCallbackForTag setValue:callback forKey:tag.key];
    [self.socket readDataToLength:length withTimeout:timeout tag:tag.longValue];
    // The callback will be invoked from the didReadData:tag: GCDAsyncSocketDelegate function
}

- (NSData *) readDataToLength:(NSUInteger)length withTimeout:(NSTimeInterval)timeout error:(NSError **)error {

    __block BOOL callbackExecuted = NO;
    __block NSData *outerData = nil;
    __block NSError *outerError = nil;
    
    [self readDataToLength:length withTimeout:timeout callback:^(NSData *data, NSError *error) {
        outerData = data;
        outerError = error;
        callbackExecuted = YES;
    }];
    
    while (!callbackExecuted) {
        [[NSRunLoop currentRunLoop] runMode:NSDefaultRunLoopMode beforeDate:[NSDate distantFuture]];
    }
    
    if (error && outerError) {
        *error = outerError;
    }
    
    return outerData;
}

- (NSData *) readDataAfterWriteData:(NSData*)data timeout:(NSTimeInterval)timeout error:(NSError **)error {
    [self writeData:data withTimeout:timeout callback:nil]; // fire and forget write, the block will happen at read time
    return [self readDataWithTimeout:timeout error:error];
}


// MARK: GCDAsyncSocketDelegate functions

- (void)socket:(GCDAsyncSocket *)sender didConnectToHost:(nonnull NSString *)host port:(uint16_t)port {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
}

- (void)socket:(GCDAsyncSocket *)sender didReadData:(NSData *)data withTag:(long)tagLongValue {
//    NSLog(@"Received call to %@ with tag %li. dataLength=%li, data=[%@], dataUTFString='%@'",
//          NSStringFromSelector(_cmd),
//          tagLongValue,
//          (unsigned long)[data length],
//          [data base64EncodedStringWithOptions:0],
//          [[NSString alloc] initWithData:data encoding:NSUTF8StringEncoding]);
    
    Tag *tag = [Tag tagWithLongValue:tagLongValue];
    void (^readCallback)(NSData *, NSError *) = [self.readCallbackForTag objectForKey:tag.key];
    if (readCallback) {
        readCallback(data, nil);
    }
    [self destroyTag:tag];
}

- (void)socket:(GCDAsyncSocket *)sender didReadPartialDataOfLength:(NSUInteger)partialLength tag:(long)tag {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
}

- (NSTimeInterval)socket:(GCDAsyncSocket *)sender shouldTimeoutReadWithTag:(long)tagLongValue elapsed:(NSTimeInterval)elapsed bytesDone:(NSUInteger)length {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
    Tag *tag = [Tag tagWithLongValue:tagLongValue];
    
    NSError *error = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorTimeout userInfo:nil];
    
    void (^readCallback)(NSData *, NSError *) = [self.readCallbackForTag objectForKey:tag.key];
    if (readCallback) {
        readCallback(nil, error);
    }
    [self destroyTag:tag];
    return 0.0; // signal to the calling GCDAsyncSocketImpl that we do not want to extend the timeout
}

- (void)socket:(GCDAsyncSocket *)sender didWriteDataWithTag:(long)tagLongValue {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
    
    Tag *tag = [Tag tagWithLongValue:tagLongValue];
    void (^writeCallback)(NSError *) = [self.writeCallbackForTag objectForKey:tag.key];
    if (writeCallback) {
        writeCallback(nil);
    }
    [self destroyTag:tag];
}

- (void)socket:(GCDAsyncSocket *)sender didWritePartialDataOfLength:(NSUInteger)partialLength tag:(long)tag {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
}

- (NSTimeInterval)socket:(GCDAsyncSocket *)sender shouldTimeoutWriteWithTag:(long)tagLongValue elapsed:(NSTimeInterval)elapsed bytesDone:(NSUInteger)length {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
    Tag *tag = [Tag tagWithLongValue:tagLongValue];
    
    NSError *error = [NSError errorWithDomain:NiFiErrorDomain code:NiFiErrorTimeout userInfo:nil];
    
    void (^writeCallback)(NSError *) = [self.writeCallbackForTag objectForKey:tag.key];
    if (writeCallback) {
        writeCallback(error);
    }
    [self destroyTag:tag];
    return 0.0; // signal to the calling GCDAsyncSocketImpl that we do not want to extend the timeout
}

- (void)socketDidSecure:(GCDAsyncSocket *)sock {
    NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
}

- (void)socketDidCloseReadStream:(GCDAsyncSocket *)sock {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
}

- (void)socketDidDisconnect:(GCDAsyncSocket *)sock withError:(NSError *)err {
    // NSLog(@"Received call to %@", NSStringFromSelector(_cmd));
}


@end
