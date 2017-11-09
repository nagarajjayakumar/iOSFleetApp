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

import UIKit

@UIApplicationMain
class AppDelegate: UIResponder, UIApplicationDelegate {

    var window: UIWindow?


    func application(_ application: UIApplication, didFinishLaunchingWithOptions launchOptions: [UIApplicationLaunchOptionsKey: Any]?) -> Bool {
        // Override point for customization after application launch.
        
        initializeUserDefaults()
        
        return true
    }

    func applicationWillResignActive(_ application: UIApplication) {
        // Sent when the application is about to move from active to inactive state. This can occur for certain types of temporary interruptions (such as an incoming phone call or SMS message) or when the user quits the application and it begins the transition to the background state.
        // Use this method to pause ongoing tasks, disable timers, and invalidate graphics rendering callbacks. Games should use this method to pause the game.
    }

    func applicationDidEnterBackground(_ application: UIApplication) {
        // Use this method to release shared resources, save user data, invalidate timers, and store enough application state information to restore your application to its current state in case it is terminated later.
        // If your application supports background execution, this method is called instead of applicationWillTerminate: when the user quits.
    }

    func applicationWillEnterForeground(_ application: UIApplication) {
        // Called as part of the transition from the background to the active state; here you can undo many of the changes made on entering the background.
    }

    func applicationDidBecomeActive(_ application: UIApplication) {
        // Restart any tasks that were paused (or not yet started) while the application was inactive. If the application was previously in the background, optionally refresh the user interface.
    }

    func applicationWillTerminate(_ application: UIApplication) {
        // Called when the application is about to terminate. Save data if appropriate. See also applicationDidEnterBackground:.
    }
    
    func initializeUserDefaults() {
        if(!UserDefaults.standard.bool(forKey: "has_launched_before")) {
            // This is the first launch ever
            UserDefaults.standard.set(true, forKey: "has_launched_before")
        }
        
        // Default config. Only gets used if these values have not been set by the user.
        let s2sConfigDefaults = [
            "nifi.s2s.config.cluster.url": "https://hsyplhdps102.amwater.net:9091",
//            "nifi.s2s.config.cluster.username": "admin",
//            "nifi.s2s.config.cluster.password": "admin-password",
            "nifi.s2s.config.portName": "From iOS",
            "nifi.s2s.config.queued.max_queued_packet_count": 1000,
            "nifi.s2s.config.queued.max_queued_packet_size": INT_MAX, // disabled
            "nifi.s2s.config.queued.preferred_batch_size": 0, // disabled
            "nifi.s2s.config.queued.preferred_batch_count": 100] as [String : Any]
        UserDefaults.standard.register(defaults: s2sConfigDefaults)
    }
    
    

}

