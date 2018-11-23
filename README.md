# libgenaro

Asynchronous Java library and CLI for encrypted file transfer on the Genaro network. It's developed on Java 8, but should be able to run on latest android. Compatible with [libgenaro](https://github.com/GenaroNetwork/libgenaro).

## Feature Finished:
- load wallet from json file with password.
- Use wallet to sign request for user authentication
- Delete bucket/rename bucket/list bucket/list files/delete file/download file
- Asynchronous I/O with concurrent peer-to-peer network requests for shards
- File encryption with AES-256-CTR
- File name and bucket name encryption with AES-256-GCM
- Asynchronous progress updates
- Seed based file encryption key for portability between devices
- File integrity and authenticity verified with HMAC-SHA512

## Feature Todo:
- Erasure encoding with reed solomon for data durability
- Retry when fail
- Better exception handle
- Cli interface

## 3rd party dependencies:
This library use Java Future/Execution framework, and following 3rd party libs.

- [Bouncy Castle](https://www.bouncycastle.org/java.html) for crypto algorithms.
- [web3j](https://github.com/web3j/web3j) for wallet managment, BIP39 and Interaction with blockchain.
- [jackson](https://github.com/FasterXML/jackson) for JSON parse/compose.
- [okhttp3](https://github.com/square/okhttp) as HTTP client.
- [guava](https://github.com/google/guava) and [apache.commons](https://commons.apache.org/) as utility.
- [log4j](https://logging.apache.org/log4j) for logging.
- [testng](https://testng.org/doc/index.html) for testing.
- [maven](https://maven.apache.org/) for dependency managment.