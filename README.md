# bw-util-hibernate [![Build Status](https://travis-ci.org/Bedework/bw-util-hibernate.svg)](https://travis-ci.org/Bedework/bw-util-hibernate)

This project provides a number of hibernate utility classes and methods for
[Bedework](https://www.apereo.org/projects/bedework).

## Requirements

1. JDK 11
2. Maven 3

## Building Locally

> mvn clean install

## Releasing

Releases of this fork are published to Maven Central via Sonatype.

To create a release, you must have:

1. Permissions to publish to the `org.bedework` groupId.
2. `gpg` installed with a published key (release artifacts are signed).

To perform a new release:

> mvn -P bedework-dev release:clean release:prepare

When prompted, select the desired version; accept the defaults for scm tag and next development version.
When the build completes, and the changes are committed and pushed successfully, execute:

> mvn -P bedework-dev release:perform

For full details, see [Sonatype's documentation for using Maven to publish releases](http://central.sonatype.org/pages/apache-maven.html).

## Release Notes
### 4.0.19
 * Split off from bw-util
    
### 4.0.20
 * Logging changes
  
### 4.0.21
 * Dependency versions
 
### 4.0.22
 * pom config changes
  
### 4.0.23
 * Update javadoc plugin config
 * Remove some references to log4j

### 4.0.24
* Update versions
* Repo for patched hibernate

### 4.0.25
* Update versions

### 4.0.26
* Update versions
* Use class loader from current thread when loading class for Enum types.

### 4.0.27
* Update versions

### 4.0.28
* Update versions

### 4.0.29
* Update versions

#### 5.0.0
* Use bedework parent
* Update versions
* Changes to fix hibernate schema issues.
* Fix so that schema gets output when file is given - even if exporting to db.

### 5.0.1
* Update versions

### 5.0.2
* Update versions

### 5.0.3
* Update versions
