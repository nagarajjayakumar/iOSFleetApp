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

#ifndef NiFiSiteToSiteDatabaseFMDB_h
#define NiFiSiteToSiteDatabaseFMDB_h

/* Visibility: Internal / Private
 *
 * This header declares classes and functionality that is only for use
 * internally in the site to site library implementation and not designed
 * for users of the site to site library.
 */

#import <Foundation/Foundation.h>
#include "NiFiSiteToSiteDatabase.h"

/********** SiteToSiteDatabase FMDB-based interfaces (defined here for testing visiblity) **********/

typedef enum {
    PERSISTENT_DEFAULT,   // Persistent to a file stored in the bundle. This is what should be used for production.
    PERSISTENT_TEMPORARY, // Persistent to a file in a temporary location that is deleted on close. Useful for testing or to change the behavior so that the local buffer is purged across app launches.
    VOLATILE_IN_MEMORY    // Volatile, only an in-memory database. Useful for testing purposes only.
} FMDBPersistenceType;


/* A concrete implementation of the NiFiSiteToSiteDatabase abstract class that leverages FMDB, a SQLite wrapper */
@interface NiFiFMDBSiteToSiteDatabase : NiFiSiteToSiteDatabase
- (nullable instancetype)init;
- (nullable instancetype)initWithPersistenceType:(FMDBPersistenceType)persistenceType;  // only for testing!
- (nullable instancetype)initWithDatabaseFilePath:(nullable NSString *)path;  // only for testing!
@end

#endif /* NiFiSiteToSiteDatabaseFMDB_h */
