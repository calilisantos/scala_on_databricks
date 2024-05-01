# Welcome to request_with_scala
This project has educational goals and aims to explore how to use **_Scala in Databricks_**, consuming an API, and dealing with the response using Spark and native solutions.

### **link to the solution in Databrick --> [here](https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3776439524338690/2632202910148279/5975377984582142/latest.html) <--**

# <a id='topics'>Topics</a>
- [The Source](#source)
- [Cluster Configs](#configs)
- [Class ReadRequest](#class)
- [How to use](#use)
- [Next steps](#next)

## <a id='source'>[The source](#topics)</a>
The **Reddit APIs** were used. It returns the top 50 stocks discussed in the Wallstreetbets subreddit over the last 15 minutes, including a sentiment analysis of the discussions. Documentation is available [here](https://tradestie.com/apps/reddit/api/).

## <a id='configs'>[Cluster Configs](#topics)</a>

For this project, DBR 13.3 was used*, and the current ENV was set to use JDK-11 on the cluster:

`JNAME=zulu11-ca-amd64` 

This JVM is necessary to enable `java.net.http.HttpRequest`** in Databricks and is required by the request libraries described below:

| Scala package | Description | Maven coordinates | Reference |
| - | - | - | - |
| **sttp.client3** | Scala library that provides HTTP request and response handlers. |`com.softwaremill.sttp.model:core_2.12:1.7.10`| [Documentation](https://sttp.softwaremill.com/en/stable/simple_sync.html) |
| **sttp.model** | Provides HTTP models such as headers, URIs, methods, etc. Required for `sttp.client`.|`com.softwaremill.sttp.tapir:tapir-sttp-client_2.12:1.10.6`| [Documentation](https://sttp.softwaremill.com/en/stable/model/model.html) |

*DBR 11.3 until 14.3 are tested and is not expected incompatibility.

**error found: `BootstrapMethodError: java.lang.NoClassDefFoundError: java/net/http/HttpRequest`. Solution find [here](https://docs.databricks.com/en/dev-tools/sdk-java.html#create-a-cluster-that-uses-jdk-17).


## <a id='class'>[Class ReadRequest](#topics)</a>
A class to deal with **_sttp.client_** Response, with attributes:
* `client`: A **SimpleHttpClient** from **_sttp.client_** instance. Used to execute the request.
* `requestEndpoint`: The endpoint informed
* `successStatusCode`: The 200 status code

And the methods:
* `getResponse`: Return the get response from the endpoint informed.
* `checkRequestStatusCode`: Raises a exception if response status code is different from 200.
* `transformResponseToDataframe`: Return a spark dataframe if request was succesfull.

## <a id='use'>[How to use](#topics)</a>
1. Upload `request_with_scala.dbc` or `request_with_scala.scala` on your Databricks Workspace;
2. Install the packages listed in [Cluster Configs](#configs);
3. Open a PR with your improvements!

## <a id='next'>[Next steps](#topics)</a>
* Use the tables for a logistic regression model.
* Made a star schema with the current layers.

