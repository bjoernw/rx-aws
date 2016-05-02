# Changes

## 1.2.0
* auto-delete was not working properly
* SQSMessage is no longer an inner class
* eliminated mapping of attributes
* eliminated JSON republishing
## 1.1.0 
* SNS Support

## 1.0.2
* Allow SQSReactorBridge to be initialized withQueueName() + SQS Client
* Defer Queue URL resolution to runtime, but cache it once it is resolved
* Remove spurious System.out.println

## 1.0.1

* Initial Release