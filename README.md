# awsME
This small package is a wrapper for boto3 based functionalities with some additional features and convenience functions.

## Main Features
The package features multithreaded up- and download of small files. In contrast to the basic functionality of boto3, that enables the user to multithread data transfer not based on chunks of the files but on the files itself. That is of particular interest for very large amounts of small files. As to it's multithreading nature, these functions can be used in multiple processes simultaneously.

Additionally, it provides a multithreaded downloading functionality for signing requests with authorization headers. That is of particular interst for AWS API Gateways secured by some Authorizer. For now, it supports SRP, MFA and API-keys for AWS Cognito user pools. Here, the user can choose a chunksize for downloading as well.

## Installation
Currently, we did not provide the package via pip or conda. However, You may install it with `pip install -e /PATH/TO/awsME-repository`.