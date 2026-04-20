# Snowflake Intelligence End-to-End Lab — Setup

This directory contains all SQL scripts needed to provision the Snowflake environment for the **Snowflake Intelligence E2E lab** from scratch and the steps needed to complete the lab.

---

## Prerequisites

Have access to one Snowflake Account where you have:

| Requirement | Value |
|---|---|
| Snowflake role | `ACCOUNTADMIN` |
| Warehouse | `COMPUTE_WH` |
| GitHub repo (public) | `https://github.com/ccarrero-sf/SI_E2E_WITH_COCO_SUMMIT26` |

---

Install Cortex Code (CoCo) CLI in your laptop

## Step 1: Create a new Workspace from the GIT repository

Select Projects -> Workspaces. Click on My Workspace and within Create click on "From Git repository"

![image](assets/image1.png)

If you do not have a API integration already created, when creating the workspace from Git repository you have the opportunity to create it. In the repository URL add:

```code
https://github.com/ccarrero-sf/SI_E2E_WITH_COCO_SUMMIT26
```

If you need to create an API Integration, click on "Create API integration", this will bring you to this window you can fill:

![image](assets/image2.png)

Or open a SQL sheet and type:

```code
CREATE OR REPLACE API INTEGRATION git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/ccarrero-sf/')
  enabled = true
  allowed_authentication_secrets = all;
```

When creating a workspace from a Git repository you have the opportunity to add your Personal access token. Because this is a public lab, just select on Public repository and click on Create.

![image](assets/image3.png)

## Step 2: Prepare all dataset to be used by this lab

Click on the 00_setup.sql script and run it all:

![image](assets/image4.png)

This script will:

-  Create or replace a new databse for this lab: CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E
-  Create stage files and copy the csv, pdf and image files from this repository
-  Parse and classify the PDF documents
-  Describe and classify the images
-  Create Snowflake tables from the CSV files
-  Create Cortex Search Services for the images, documents and customer feedback

This is what we should have to start our lab:

![image](assets/image5.png)


## Step 3: Install Cortex Code (CoCo) CLI

Follow the official [installation guide](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) to install and configure Cortex Code CLI.

The first prompt asks you to choose a connection from the existing connections in the ~/.snowflake/connections.toml file or to create a new connection.

As you will be connecting to a demo account provisioned for you during this lab, you will have to create a new connection, choose More options* by pressing the down arrow key until it is highlighted, then press Enter. Follow the prompts to enter your Snowflake account details.

If you have already used CoCo, you want the Agent Connection and SQL Connection to leverage the Snowflake account you are using for this lab. Run cortex and type /connections and add a new one:

![image](assets/image6.png)

## Step 4: Connect CoCo with the Summit Slack Channel

In this step, we are going to connect CoCo with a specific demo Slack channel we are going to be using fo this lab. The goal is that CoCo can provide notifictions about the work it is doing.

You may want to create in your laptop one specific folder for this lab. Go ther, and call cortex:

```code
cortex
```

Ask CoCo with this prompt to make this connection (the specific Team ID and Token will be provided to you separately):

```
Leverage the MCP server connection to Slack so you can write messages to the channel summit_HOL. Create a connection with the Team ID: T08-------CC and
Oauth Token: xoxb-875--------------------------------------vs

Write a message to the channel saying hello I am <add your name for CoCo> and what account you are connected to.
````

After this, your lab instructor should start getting messages in the slack channel like this:

![image](assets/image7.png)

## Step 5: Create a Semantic Model using CoCo

Our fist step is to create a Semantic Model that can be used by our agents to ask questions in natural language to query the Snowflake tables. We are going to ask CoCo to make that work. In the cortex session you have open, lets do the following:

First enable /plan mode, so we can verify what will be done

> /plan

Now enter this prompt 


```code
We have the database CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E where we already have tables with sales data. We want to create a semantic model on the Snowflake tables we have so it will be used later with one Snowflake Agent. The semantic view should include some business logic and terminology. The stage area contains one file with examples of questions we wanted to ask and that we will be using later for validation.

Everything should be scripted so we can reproduce it anytime. Enumerate it.. Create all under a folder called output.

Once you have created all the scripts needed, run them and check they are ok. Correct any failure.

Create a README (add a suffix with the name of the step) file with the steps you took here.

Once you are done, send a message to the summit_HOL slack channel with a brief summary of what you are done. Identify who you are in the message.
```

Observe how CoCo is automatically selecting the right skills and interacting with the data to build the plan for the Semantic View. It may even get some errors but it will re-think and correct them.


Your output may differ as the ouput of LLMs are not deterministic. but this is the plan suggested in this run:

![image](assets/image8.png)

If you are ok with the plan suggested go ahead and confirm it. 

Once the implemtation is done, your lab instructor should have received a slack message with what has been done:

![image](assets/image9.png)

It would be possible that once you try to use the Semantic View it produces some errors. If that is the case, you can just ask CoCo to correct that error. For example, in this case, the first Semantic Model produced got some errors. Ask CoCo to fix your error (this is just one examle, do not copy paste from here):

```code
When using the semantic model we got this error:
```

>Unable to parse yaml to protobuf. Error: Failed to parse tables field: Failed to parse metrics field: Message type "com.snowflake.cortex.analyst.Metric" has no field named "default_aggregation" at "SemanticModel.tables[0].metrics[0]". Available Fields(except extensions): "['name', 'synonyms', 'description', 'expr', 'filter', 'accessModifier', 'tags', 'nonAdditiveDimensions', 'additiveDimensions', 'usingRelationships']"..

And CoCo corrected the error in our case:

![image](assets/image10.png)

You can test the Semantic View going to AI & ML ->  Analyst and select your Semantic Model. In the Playground you can ask questions to test it.

## Step 6: Create an Agent

Now we want CoCo to help us builindg and Agent that will be able to leverate the information we have both in images, pdf couments and Snowflake table. The agent shoudl be able to use all that information in order to respond to different questions about the product and sales.

We also want to agent to have the capability of sending emails at the end.

This is the prompt we will be using (copy/past):

```code
We have the database CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E
Where we want to build one Snowflake Agent that will leverage the Cortex Search Tools and the Semantic View available in that database. The Agent is a Sales Expert for the retailer company. Should be able to respond to product specification questions and sales about those products. It should be able to also dig into customer issues and understand differences in sales between products. 

The Agent is multi-lingual and should always respond in the same language the questions are being asked.

The Agent should also finish asking if a summary should be sent by email and have a tool to send it.

The Agent will be used with Snowflake Intelligence.

Create a the scripts with those steps and check it runs ok.

Create another README file for this work with the steps you took

Once you are done, send a message to the summit_HOL slack channel with a brief summary of what you are done. Identify who you are in the message.
```

In the proposed plan, we can see how in our case, it has identified the Semantic View we just created, plus the Cortex Search service for the documents and customer feedback that was created by the first script, plues the custom tool to send email. 

![image](assets/image11.png)

When it is done, CoCo explain a summary of what is done, but as instructed, we also have it in our Slack channel:

![image](assets/image12.png)

## Step 7: AI Observability

<pre style="white-space: pre-wrap;">
```code
We have the database CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E where one Snowflake Agent has already been configured. We want to leverage the AI Observability for Agents for Snowflake in order to validate that agent. Create the table needed by AI_OBSERVABILITY with these questions, valid answers and the tools used:

Question: What is the bike with more revenue last year and what is their guarantee
Answer: The Mondracer Infant Bike was the bike with the highest revenue last year (2025), generating $16,353,000 USD in total sales. The guarantee is 3 years
Tools: Analytical tool and search tool for products

Question: Who tested the ski boots?
Answer: The Cerler-Aneto ski club
Tools. Search tool for product

Question: What is the ski with highest and lowest sales for the entire period?
Answer: Highest sales: Racing Fast Skis with $580,450 in total revenue. Lowest sales: Carver Skis with $500,070 in total revenue
Tools. Analytical tool

Question: What is the product with lower sales and what is the feedback that we have
Answer: Ski Boots TDBootz Special. Good feedback for comfort, fit, durability, 

Create all the scripts with those steps and check it runs ok.

Create another README file for this work with the steps you took

Once you are done, send a message to the summit_HOL slack chanel with a brief summary of what you are done. Identify who you are in the message.
```
</pre>

You can observe that CoCo is also managing the executions of the evaluations. Message in the Slack channel:

![image](assets/image13.png)

If you click on AI & ML --> Agents and select the SALES_EXPERT_AGENT, you can see the evaluations we have:

![image](assets/image14.png)
