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

#ifndef NiFiSiteToSiteTransaction_h
#define NiFiSiteToSiteTransaction_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 */

#import <Foundation/Foundation.h>
#import "NiFiSiteToSite.h"

// MARK: - Internal Transaction Enums and Interface

typedef enum {
    RESERVED = 0, // in case we need to extend the length of response code,
    // so that we can indicate a 0 followed by some other bytes
    
    // handshaking properties
    PROPERTIES_OK = 1,                // (1, "Properties OK", false)
    UNKNOWN_PROPERTY_NAME = 230,      // (230, "Unknown Property Name", true)
    ILLEGAL_PROPERTY_VALUE = 231,     // (231, "Illegal Property Value", true)
    MISSING_PROPERTY = 232,           // (232, "Missing Property", true),
    // transaction indicators
    CONTINUE_TRANSACTION = 10,        // (10, "Continue Transaction", false),
    FINISH_TRANSACTION = 11,          // (11, "Finish Transaction", false),
    CONFIRM_TRANSACTION = 12,         // (12, "Confirm Transaction", true), // "Explanation" of this code is the checksum
    TRANSACTION_FINISHED = 13,        // (13, "Transaction Finished", false),
    TRANSACTION_FINISHED_BUT_DESTINATION_FULL = 14, // (14, "Transaction Finished But Destination is Full", false),
    CANCEL_TRANSACTION = 15,          // (15, "Cancel Transaction", true),
    BAD_CHECKSUM = 19,                // (19, "Bad Checksum", false),
    // data availability indicators
    MORE_DATA = 20,                   // (20, "More Data Exists", false),
    NO_MORE_DATA = 21,                // (21, "No More Data Exists", false),
    // port state indicators
    UNKNOWN_PORT = 200,               // (200, "Unknown Port", false),
    PORT_NOT_IN_VALID_STATE = 201,    // (201, "Port Not in a Valid State", true),
    PORTS_DESTINATION_FULL = 202,     // (202, "Port's Destination is Full", false),
    // authorization
    UNAUTHORIZED = 240,               // (240, "User Not Authorized", true),
    // error indicators
    ABORT = 250,                      // (250, "Abort", true),
    UNRECOGNIZED_RESPONSE_CODE = 254, // (254, "Unrecognized Response Code", false),
    END_OF_STREAM = 255               // (255, "End of Stream", false);
} NiFiTransactionResponseCode;

@interface NiFiTransactionResult()
@property (nonatomic, readwrite) NiFiTransactionResponseCode responseCode;
@property (nonatomic, readwrite) uint64_t dataPacketsTransferred;
@property (nonatomic, assign, readwrite, nullable) NSString *message;
@property (nonatomic, readwrite) NSTimeInterval duration;
- (nonnull instancetype)init;
- (nonnull instancetype)initWithResponseCode:(NiFiTransactionResponseCode)responseCode
                      dataPacketsTransferred:(NSUInteger)packetCount
                                     message:(nullable NSString *)message
                                    duration:(NSTimeInterval)duration;
@end

#endif /* NiFiSiteToSiteTransaction_h */
