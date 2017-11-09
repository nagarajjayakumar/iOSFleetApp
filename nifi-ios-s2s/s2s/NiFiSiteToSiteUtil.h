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


#ifndef NiFiSiteToSiteUtil_h
#define NiFiSiteToSiteUtil_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 *
 */

#import <Foundation/Foundation.h>
#import "NiFiSiteToSite.h"

// MARK: - SiteToSite Util

@interface NiFiSiteToSiteUtil : NSObject
+ (nonnull NSString *)NiFiTransactionStateToString:(NiFiTransactionState)state;
@end

#endif /* NiFiSiteToSiteUtil_h */
