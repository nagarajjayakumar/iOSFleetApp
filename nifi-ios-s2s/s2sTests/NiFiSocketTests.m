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

#import <XCTest/XCTest.h>
#import "NiFiSocket.h"

// MARK: - GCDAsyncSocket Mock

@protocol GCDAsyncSocketDelegateProtocol <NSObject>
- (void)socket:(id)sender didWriteDataWithTag:(long)tag;
- (void)socket:(id)sender didReadData:(NSData *)data withTag:(long)tag;
@end

@protocol GCDAsyncSocketProtocol <NSObject>
- (void)setDelegate:(id<GCDAsyncSocketDelegateProtocol>)delegate;
- (void)setDelegateQueue:(dispatch_queue_t)delegateQueue;
- (BOOL)connectToHost:(NSString *)host onPort:(uint16_t)port error:(NSError **)errPtr;
- (void)disconnectAfterReadingAndWriting;
- (void)startTLS:(NSDictionary *)tlsSettings;
@end

@interface MockGCDAsyncSocket : NSObject <GCDAsyncSocketProtocol>
@property id delegate;
@property NSMutableDictionary<NSString *, NSNumber *> *callCountPerSelector;
@end

@implementation MockGCDAsyncSocket

@synthesize delegate = _delegate;

-(instancetype)init {
    self = [super init];
    if (self) {
        _callCountPerSelector = [NSMutableDictionary dictionary];
    }
    return self;
}

- (void) incrementCallCountForSelectorString:(NSString *)sel {
    _callCountPerSelector[sel] = _callCountPerSelector[sel] ? @([_callCountPerSelector[sel] integerValue] + 1) : @1;
}

- (void)setDelegate:(id<GCDAsyncSocketDelegateProtocol>)delegate {
    _delegate = delegate;
    [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)];
}

- (id<GCDAsyncSocketDelegateProtocol>)delegate {
    [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)];
    return _delegate;
}

- (void)setDelegateQueue:(dispatch_queue_t)delegateQueue { [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)]; }

- (BOOL)connectToHost:(NSString *)host onPort:(uint16_t)port error:(NSError **)errPtr {
    [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)];
    return YES;
}

- (void)disconnectAfterReadingAndWriting { [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)]; }

- (void)startTLS:(NSDictionary *)tlsSettings { [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)]; }

- (void)writeData:(NSData *)data withTimeout:(NSTimeInterval)timeout tag:(long)tag {
    [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)];
    [self.delegate socket:self didWriteDataWithTag:tag];
}

- (void)readDataWithTimeout:(NSTimeInterval)timeout tag:(long)tag {
    [self incrementCallCountForSelectorString:NSStringFromSelector(_cmd)];
    [self.delegate socket:self didReadData:[@"Data" dataUsingEncoding:NSUTF8StringEncoding] withTag:tag];
}

@end



// MARK: - NiFiSocket expose private interface methods for testing

@interface NiFiSocket()
- (nullable instancetype) initWithAsyncSocket:(NSObject<GCDAsyncSocketProtocol> *)socket;
@end



// MARK: - NiFiSocketTests

@interface NiFiSocketTests : XCTestCase
@end

@implementation NiFiSocketTests

- (void)setUp {
    [super setUp];
    // Put setup code here. This method is called before the invocation of each test method in the class.
}

- (void)tearDown {
    // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
}

- (void)testConnectToHost {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    XCTAssertNotNil(socket);
    
    BOOL connected = [socket connectToHost:@"localhost" onPort:0 error:nil];
    XCTAssertEqual(YES, connected);
    XCTAssertTrue([asyncSocket.callCountPerSelector[@"connectToHost:onPort:error:"] isEqualToNumber:@1]);
}

- (void)testDisconnect {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    XCTAssertNotNil(socket);
    
    [socket disconnect];
    XCTAssertTrue([asyncSocket.callCountPerSelector[@"disconnectAfterReadingAndWriting"] isEqualToNumber:@1]);
}

- (void)testStartTLS {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    [socket connectToHost:@"localhost" onPort:0 error:nil];
    XCTAssertNotNil(socket);
    
    [socket startTLS:nil];
    XCTAssertTrue([asyncSocket.callCountPerSelector[@"startTLS:"] isEqualToNumber:@1]);
}

- (void)testWriteData {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    [socket connectToHost:@"localhost" onPort:0 error:nil];
    XCTAssertNotNil(socket);
    
    [socket writeData:[@"Data" dataUsingEncoding:NSUTF8StringEncoding] withTimeout:0.1 error:nil];
    XCTAssertTrue([asyncSocket.callCountPerSelector[@"writeData:withTimeout:tag:"] isEqualToNumber:@1]);
}

- (void)testWriteDataAsync {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    [socket connectToHost:@"localhost" onPort:0 error:nil];
    XCTAssertNotNil(socket);
    
    [socket writeData:[@"Data" dataUsingEncoding:NSUTF8StringEncoding] withTimeout:0.1 callback:^(NSError *error) {
        XCTAssertTrue([asyncSocket.callCountPerSelector[@"writeData:withTimeout:tag:"] isEqualToNumber:@1]);
    }];
}

- (void)testReadData {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    [socket connectToHost:@"localhost" onPort:0 error:nil];
    XCTAssertNotNil(socket);
    
    NSData *data = [socket readDataWithTimeout:0.1 error:nil];
    XCTAssertNotNil(data);
    XCTAssertTrue([asyncSocket.callCountPerSelector[@"readDataWithTimeout:tag:"] isEqualToNumber:@1]);
}

- (void)testReadDataAsync {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    [socket connectToHost:@"localhost" onPort:0 error:nil];
    XCTAssertNotNil(socket);
    
    [socket readDataWithTimeout:0.1 callback:^(NSData *data, NSError *error) {
        XCTAssertNotNil(data);
        XCTAssertTrue([asyncSocket.callCountPerSelector[@"readDataWithTimeout:tag:"] isEqualToNumber:@1]);
    }];
}

- (void)testReadDataAfterWrite {
    MockGCDAsyncSocket *asyncSocket = [[MockGCDAsyncSocket alloc] init];
    NiFiSocket *socket = [[NiFiSocket alloc] initWithAsyncSocket:asyncSocket];
    
    [socket connectToHost:@"localhost" onPort:0 error:nil];
    XCTAssertNotNil(socket);
    
    NSData *data = [socket readDataAfterWriteData:[@"Data" dataUsingEncoding:NSUTF8StringEncoding] timeout:0.1 error:nil];
    XCTAssertNotNil(data);
    XCTAssertTrue([asyncSocket.callCountPerSelector[@"writeData:withTimeout:tag:"] isEqualToNumber:@1]);
    XCTAssertTrue([asyncSocket.callCountPerSelector[@"readDataWithTimeout:tag:"] isEqualToNumber:@1]);
}




@end
