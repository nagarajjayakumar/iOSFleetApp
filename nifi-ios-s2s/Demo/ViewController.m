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

#import "ViewController.h"
#import "s2s.h"

@interface ViewController ()
@property NSInteger totalFlowFileCount;
@property (strong, nullable) NiFiSiteToSiteClient *s2sClient;
@end

@interface URLSessionAuthenticatorDelegate : NSObject <NSURLSessionDelegate>
- (void)URLSession:(NSURLSession *)session didReceiveChallenge:(NSURLAuthenticationChallenge *)challenge
 completionHandler:(void (^)(NSURLSessionAuthChallengeDisposition disposition, NSURLCredential * _Nullable credential))completionHandler;
@end

@implementation ViewController

- (void)viewDidLoad {
    [super viewDidLoad];
    [[self view] setBackgroundColor:[UIColor colorWithRed:217.0f/255.0f green:217.0f/255.0f blue:217.0f/255.0f alpha:1.0]];
    
    /* NiFi config. Note, rather than hardcoded here, this could be loaded from another config source, e.g., a .plist resource file or UserDefaults */
    NiFiSiteToSiteRemoteClusterConfig *remoteNiFiInstance = [NiFiSiteToSiteRemoteClusterConfig configWithUrl:[NSURL URLWithString:@"http://localhost:8080"]];
    // remoteNiFiInstance.username = @"admin";
    // remoteNiFiInstance.password = @"admin-password";
    
    /* add a url session delegate that handles custom server TLS chain validation (not needed for cert signed by root CA) */
    // remoteNiFiInstance.urlSessionDelegate = [[URLSessionAuthenticatorDelegate alloc] init];
    
    NiFiSiteToSiteClientConfig *s2sConfig = [NiFiSiteToSiteClientConfig configWithRemoteCluster: remoteNiFiInstance];
    s2sConfig.portName = @"From iOS";
    
    _totalFlowFileCount = 0;
    _s2sClient = [NiFiSiteToSiteClient clientWithConfig:s2sConfig];
    // Configured client is now ready to create transactions
}


- (void)didReceiveMemoryWarning {
    [super didReceiveMemoryWarning];
    // Dispose of any resources that can be recreated.
}

- (IBAction)handleSendButtonClick:(id)sender {
    
    // Make sure we can do the requested action
    if (_s2sClient == nil) {
        return;
    }
    if (_userTextField.text == nil) {
        return;
    }
    
    // Create Site-to-Site Transactionh
    id transaction = [_s2sClient createTransaction];
    
    // Send Data Packet(s) over Transaction
    NiFiDataPacket *textFlowFile = [NiFiDataPacket dataPacketWithString:_userTextField.text];
    [transaction sendData:textFlowFile];
    
    // Complete Transaction
    NiFiTransactionResult *result = [transaction confirmAndCompleteOrError:nil];
    
    // Update Flow File counter and View lable
    _totalFlowFileCount += result.dataPacketsTransferred;
    _ffCountLabel.text = [NSString stringWithFormat:@"%ld flow files sent so far", (long)_totalFlowFileCount];
}


@end


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
@implementation URLSessionAuthenticatorDelegate

- (void)URLSession:(NSURLSession *)session didReceiveChallenge:(NSURLAuthenticationChallenge *)challenge
 completionHandler:(void (^)(NSURLSessionAuthChallengeDisposition disposition, NSURLCredential * _Nullable credential))completionHandler {
    
    completionHandler(NSURLSessionAuthChallengeUseCredential, [NSURLCredential credentialWithUser:@""
                                                                                         password:@""
                                                                                      persistence:NSURLCredentialPersistenceForSession]);
}
@end

