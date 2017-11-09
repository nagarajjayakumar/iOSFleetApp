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
#import "NiFiSiteToSiteUtil.h"

@implementation NiFiSiteToSiteUtil

+ (nonnull NSString *) NiFiTransactionStateToString:(NiFiTransactionState)state {
    switch(state) {
        case TRANSACTION_STARTED:
            return @"TRANSACTION_STARTED";
        case DATA_EXCHANGED:
            return @"DATA_EXCHANGED";
        case TRANSACTION_CONFIRMED:
            return @"TRANSACTION_CONFIRMED";
        case TRANSACTION_COMPLETED:
            return @"TRANSACTION_COMPLETED";
        case TRANSACTION_CANCELED:
            return @"TRANSACTION_CANCELED";
        case TRANSACTION_ERROR:
            return @"TRANSACTION_ERROR";
        default:
            @throw [NSException
                    exceptionWithName:NSGenericException
                    reason:@"Unexpected NiFiTransactionState."
                    userInfo:nil];
    }
}

@end
