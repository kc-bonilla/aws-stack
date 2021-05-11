# ETL Workflow Design in AWS Step Functions
 
[stepfunctions_graph.pdf](https://github.com/kc-bonilla/aws_stepfunction_complex_etl_workflow/files/6456091/stepfunctions_graph.pdf)

# **WIKI**

## **Extract Jobs Workflow**

## AWS Step Function Documentation

**Adding a New Job to the State Machine**

To add, edit, or remove a State Machine extract job, transform job, crawler, or activity:

1. Navigate to the [S3 overview homepage](http://console.aws.amazon.com/s3/)

1. Locate the input file in its respective S3 Bucket by selecting:

**datapipeline-analytics-shootproof-com**  **step-functions/**  **scripts/**  **SF\_Input\_Items.json**

1. Select **Object**** Actions **** Download**

1. After the download finishes, open the file and locate the subschema within the JSON body that the job belongs to. For example, if adding a Glue job for a ShootProof extract, the job should be added under the JSON subschema **SP\_ExtractJobs** as:

**{**

&quot; **JobName&quot;: &quot;shootproof\_new\_extract\_job\_example&quot;**

**}**

- All jobs listed under the **SP\_ExtractJobs** and **Tave\_ExtractJobs** subschemas use an identical &quot;JobName&quot; key as they are passed during Map States in the State Machine and require a![stepfunctions_graph](https://user-images.githubusercontent.com/74749648/117748808-4a252c80-b1d6-11eb-8e3c-18e1e5c68a30.png)
n identical key; however, all other inputs outside of these two subschemas are not called from Map States and thus need a unique key when added.

- Example:

To add a new transform proc/Glue job that performs transforms on Pendo data, the following would need to be added under the **TransformJobs** subschema in the input file&#39;s JSON body:

**{**

**&quot;pendo&quot;: &quot;pendo\_transform&quot;**

**}**

This new input would then need to be referenced in the State Machine&#39;s main Amazon States Language code (in the appropriate State&#39;s **Parameters** JSON) with the JSONPath†:

&quot; **JobName.$&quot; : &quot;$.TransformJobs.pendo&quot;**

1. Once finished making changes, upload the amended input file back to its S3 bucket, replacing the older version with the amended version. It is important that the file name remain unchanged unless (detailed in section &#39;Updating the Filename or S3 Location of the State Machine Input&#39;).

† [_ **http://jsonpath.com/** _](http://jsonpath.com/) _ **is a useful tool for verifying the evaluation of your JSONPath expression** _

**Updating the State Machine&#39;s Schedule**

To make changes to the State Machine&#39;s scheduled run time or frequency:

1. Navigate to the [CloudWatch overview homepage](https://console.aws.amazon.com/cloudwatch/)

1. Under the **Events** section on the left-hand side of the screen, select:

**Rules**  **RunExtracts**  **Actions** (in top right corner)  **Edit** (from drop-down)

1. Change **Cron**** expression** to the desired schedule/cadence††

- Example:

To set the State Machine to run daily at 1:15 AM EST, the input for &#39;Cron expression&#39; should correspond with 6:15 AM UTC and should read:

1. **6 \* \* ? \***

- Additional examples and documentation on cron expressions are available [here](https://docs.aws.amazon.com/AmazonCloudWatch/latest/events/ScheduledEvents.html)

†† _ **Times are in UTC/GMT, which is 5 hours ahead of EST** _

**Updating the Filename or S3 Location of the State Machine Input**

1. Navigate to the [CloudWatch overview homepage](https://console.aws.amazon.com/cloudwatch/)

1. Select the Lambda function **PassInput**

1. Change the S3 location of the input file by modifying the **Bucket** and **Key** variables to match the bucket and key of the new S3 location.

1. Under the **Events** section on the left-hand side of the screen, select:

**Rules**  **RunExtracts**  **Actions** (in top right corner)  **Edit** (from drop-down)

1. Under the console&#39;s **Targets** panel in the **Configure inputs** section, change the input in the **Constant (JSON text)** input field to the path of the S3 bucket where the input file is now located.

- Example:

If you created a new S3 folder **extracts\_wf\_inputs** within the existing **datapipeline-analytics-shootproof-com/step-functions** folder structure and wanted to store the State Machine input file under the new filename **inputs.json** , you would make the following changes:

**{&quot;Data&quot;: &quot;arn:aws:s3:::datapipeline-analytics-shootproof-com/step-functions/** ~~**scripts/SF\_Input\_Items.json**~~ **&quot;}**



**{&quot;Data&quot;: &quot;arn:aws:s3:::datapipeline-analytics-shootproof-com/step\_functions/**** extracts\_wf\_inputs/inputs.json ****&quot;}**

1. Update the DAE Wiki to reflect the new S3 location and filename of the input file **.**
