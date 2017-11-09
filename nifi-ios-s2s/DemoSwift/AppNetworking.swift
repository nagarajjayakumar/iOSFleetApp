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

import Foundation

class AppNetworkingUtils: NSObject {
    
    static func demoUrlSessionConfiguration() -> URLSessionConfiguration {
        let config = URLSessionConfiguration.default
        
        // Optionally, this configuration can allow for background data task completion, as described in "Downloading Content in the Background" here:
        // https://developer.apple.com/library/content/documentation/iPhone/Conceptual/iPhoneOSProgrammingGuide/BackgroundExecution/BackgroundExecution.html
        
        return config
    }

}

/** Below you will find a simple example of a NSURLSessionDelegate that will accept self-signed certificates.
 ** THIS IS FOR DEMO PURPOSES ONLY AND SHOULD NOT BE USED IN PRODUCTION AS IT IS NOT SECURE.
 **
 ** In production, it is recommended to use a certificate signed by a trusted root Certificate Authority, which
 ** will not require implementing your own idenitity verification methods (i.e., https when using a CA-signed
 ** certificate will "just work".
 **
 ** If, in a real-world deployment, you must use a self-signed certificate or perform custom TLS chain validatation
 ** for any reason , do not use this demo code and instead follow Apple's Guidelines in the following article under
 ** the "Performing Custom TLS Chain Validation" section:
 ** https://developer.apple.com/library/content/documentation/Cocoa/Conceptual/URLLoadingSystem/Articles/AuthenticationChallenges.html
 **
 ** For more information, please see Apple's developer documentation:
 ** https://developer.apple.com/library/content/technotes/tn2232/_index.html
 **/
class AppURLSessionDelegate: NSObject, URLSessionDelegate {
    
    func urlSession(_ session: URLSession,
         didReceive challenge: URLAuthenticationChallenge,
            completionHandler: @escaping (URLSession.AuthChallengeDisposition, URLCredential?) -> Void) {
        
        // If this were real-world code, you would do server cert TLS-chain validation here,
        // e.g., load a CA cert.der file from your app bundle, extract it's CA chain, compare
        // it to the chain provided by the server using Apple's SecTrustEvaluate API to evaluate it
        // and asserting the result is not an error value.
    
        completionHandler(.useCredential, URLCredential(user:"", password:"", persistence:URLCredential.Persistence.forSession))
        
        // NOTE: this implementation alone is not enough to do the self-signed cert. Must also add "Allow Arbitrary Loads":YES to
        // "App Transport Security Settings" dict in Info.plist
    }
}
