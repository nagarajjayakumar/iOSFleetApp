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

#ifndef NiFiError_h
#define NiFiError_h

/* Visibility: External / Public
 *
 * This header defines a public interface of the s2s framework / module.
 */

#import <Foundation/NSError.h>

FOUNDATION_EXPORT NSErrorDomain const NiFiErrorDomain;

/*!
 @enum NiFi-related Error Codes
 @abstract Constants used by NSError to indicate errors in the NiFi domain
 */
NS_ENUM(NSInteger)
{
    NiFiErrorUnknown = -1,
    
    // Miscelleneous Errors
    NiFiErrorTimeout = 100,
    
    // HTTP Errors
    NiFiErrorHttpStatusCode = 1000, // note, 1000-1999 are reserved for errors relating to HTTP status codes
                                    // to pass the HTTP Status code in the error code, you can add it to this,
                                    // e.g., 404 becomes 1404 (= 1000 + 404)
                                    // To extract the HTTP Status code, you can do:
                                    //   int httpStatusCode = errorStatusCode - NiFiErrorHttpStatusCode
    
    // Site-to-Site
    NiFiErrorSiteToSiteClient = 2000,
    NiFiErrorSiteToSiteClientCouldNotCreateTransaction = 2001,
    NiFiErrorSiteToSiteClientCouldNotLookupSiteToSiteInfo = 2002,
    NiFiErrorSiteToSiteClientCouldNotLookupInputPorts = 2003,
    NiFiErrorSiteToSiteClientCouldNotLookupPeers= 2004,
    
    // Site-to-Site Transaction
    NiFiErrorSiteToSiteTransaction = 3000,
    NiFiErrorSiteToSiteTransactionInvalidServerResponse = 3001,

    // Site-to-Site Database
    NiFiErrorSiteToSiteDatabase = 4000,
    NiFiErrorSiteToSiteDatabaseReadFailed = 4001,
    NiFiErrorSiteToSiteDatabaseWriteFailed = 4002,
    NiFiErrorSiteToSiteDatabaseTransactionFailed = 4003,
    
    // HTTP Rest API Client
    NiFiErrorHttpRestApiClient = 5000,
    NiFiErrorHttpRestApiClientCouldNotFormURL = 5001,
    
    
};

#endif /* NiFiError_h */
