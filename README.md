## Payload Offloading Java Common Library For AWS

The **Payload Offloading Java Common Library For AWS** enables you to manage payloads with Amazon S3. 
For example, this is useful for storing and retrieving payloads with size greater than the current SQS/SNS limit of 256 KB, up to a maximum of 2 GB.
 
You can use this library to:

* Specify whether payloads are always stored in Amazon S3 or only when a payload's size exceeds 256 KB.

* Get the corresponding payload object from an Amazon S3 bucket.

* Delete the corresponding payload object from an Amazon S3 bucket.

You can download release builds through the [releases section of this](https://github.com/awslabs/large-payload-offloading-java-common-lib-for-aws/releases) project.

## Getting Started

* **Sign up for AWS** -- Before using this library with Amazon services, you need an AWS account. For more information about creating an AWS account and retrieving your AWS credentials, see [AWS Account and Credentials](http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/java-dg-setup.html) in the AWS SDK for Java Developer Guide.
* **Minimum requirements** -- You'll need Java 8 (or later) and [Maven 3](http://maven.apache.org/).
* **Download** -- Download the [latest preview release](https://github.com/awslabs/large-payload-offloading-java-common-lib-for-aws/releases) or pick it up from Maven:

### Version 2.x
```xml
  <dependency>
    <groupId>software.amazon.payloadoffloading</groupId>
    <artifactId>payloadoffloading-common</artifactId>
    <version>2.0.0</version>
  </dependency>
```   

### Version 1.x
```xml
  <dependency>
    <groupId>software.amazon.payloadoffloading</groupId>
    <artifactId>payloadoffloading-common</artifactId>
    <version>1.1.0</version>
  </dependency>
```                                                                                                                     

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

