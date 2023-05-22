Welcome to template dbt project showcasing two packages for data vault automation.

### What you will find inside
This demo project contains four source csv files, which you can load into your data warehouse by running `dbt seed`.
All models are building on top of these source tables in following layer structure:
- psa -> persistent staging area,
- stage -> staging layer preparing data for raw data vault load,
- raw_vault -> raw data vault layer.

Here is a diagram of a simple model based on our source tables. It is a structure, we want to build with models in this repository.

Stage and raw_vault layers are split into two folders, named after used packages. One of the goals of this repository is to show differences between the two packages, which we achieve by using same macros from two different packages, so we can see how they differ.

### Using the template
Before running any models, you will need to install `automate_dv` and `datavault4dbt` packages by running `dbt deps`.
All models should work as it is. Feel free to play around with them to figure out how thing work. In `dbt_project.yml` file, there are plenty of variables that can be laveraged to customise behaviour of both packages, we suggest to go through them to see how much you can change the respective frameworks without touching the macros.

### Data quality packages by Infinite Lambda
While we don't include any tests in this repository, in real project we strongly recommend having high test coverage. 
To help you understand data quality issues in your project and test coverage of your data vault project, we suggest checking our following packages:
- [dq_tools](https://hub.getdbt.com/infinitelambda/dq_tools/latest/)
- [dq_vault](https://hub.getdbt.com/infinitelambda/dq_vault/latest/)

