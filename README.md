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

** Create or replace a new databse for this lab: CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E
** Create stage files and copy the csv, pdf and image files from this repository
** Parse and classify the PDF documents
** Describe and classify the images
** Create Snowflake tables from the CSV files
** Create Cortex Search Services for the images, documents and customer feedback

This is what we should have to start our lab:

![image](assets/image5.png)


## Step 3: Install Cortex Code (CoCo) CLI

Follow the official [installation guide](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli) to install and configure Cortex Code CLI.

The first prompt asks you to choose a connection from the existing connections in the ~/.snowflake/connections.toml file or to create a new connection.

As you will be connecting to a demo account provisioned for you during this lab, you will have to create a new connection, choose More options* by pressing the down arrow key until it is highlighted, then press Enter. Follow the prompts to enter your Snowflake account details.

