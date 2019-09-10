#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd 
import numpy as np 
import json 
from datetime import datetime, date 
import hashlib
import requests
from pandas.io.json import json_normalize
import sqlalchemy as sa


# In[2]:


basics = {"domain" : "eu.shibumi.com",
"client_id" : "329c7049-681c-4df9-bddf-083a24fe51f5",
"client_secret" : "df4d4791-c1bd-46b5-a345-356a494f987c",
"username": "api-capita-ctt-etl@shibumi.com",
"password":"d4Wmt71un2FKPPdhQf5wxwB!zQqac",
"enterprise" : "a2db7acf-b61c-4cec-af46-ff7d10c52a22"}


# In[10]:


url = "https://eu.shibumi.com/api/2.0/enterprise/a2db7acf-b61c-4cec-af46-ff7d10c52a22/GraphQL/graphQL"


# In[102]:


query  = '''{
  workItems {
    Programme: CTT_2_0_Programme__t(id: "fe51d041-9587-42a7-9a68-8f11087bac00") {
      name
      descendants {
        Division: CTT_2_0_Division__t {
          DivisionName:name
          descendants {
            BU_Level_4: CTT_2_0_BU_Level_4__t {
              BU_Level_4Name:name
              descendants {
                BU_Level_5: CTT_2_0_BU_Level_5__t {
                  BU_Level_5Name:name
                  descendants {
                    Initiative: CTT_2_0_Initiative__t {
                      name
                      Initiative_ID: autoNumber
                      description
                      Savings_Measure__c
                      Workstream__c
                      Lever__c
                      Sub_lever__c
                      Sub_lever_if_Other_please_specify__c
                      Cost_package__c
                      Cost_centre__c
                      Initiative_sponsor__c
                      Initiative_interdependencies__c
                      Target_FTE: Target_cost_savings_issued_to_the_initit__c
                      Target_cost_savings_issued_to_the_initia__c
                      Target_cost_to_achieve_given_to_the_init__c
                      Initiative_current_addressable_base_FTE__c
                      Initiative_current_addressable_base__c
                      Current_RAG_status__c
                      Evidence_Status__c
                      Comments__c
                      Weighted_Pipeline_Saving_FTE: metric(name: "Weighted Pipeline Saving (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      CTA_2018_Run_rate_GBP: metric(name: "CTA 2018 Run rate (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Weighted_Pipeline_GBP: metric(name: "Weighted Pipeline (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      CTA_2019_Run_rate_GBP: metric(name: "CTA 2019 Run rate (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Severance_costs_GBP: metric(name: "Severance costs (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Offshore_transition_costs_GBP: metric(name: "Offshore transition costs (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Other_costs_to_achieve_GBP: metric(name: "Other costs to achieve (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Central_Programme_costs_GBP: metric(name: "Central Programme costs (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Invest_to_Save_GBP: metric(name: "Invest to Save (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Unweighted_Pipeline_Saving_FTE: metric(name: "Unweighted Pipeline Saving (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Unweighted_Pipeline_GBP: metric(name: "Unweighted Pipeline (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Validated_FTE: metric(name: "Validated (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Validated_GBP: metric(name: "Validated (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Actioned_FTE: metric(name: "Actioned (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Actioned_GBP: metric(name: "Actioned (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Evidenced_FTE: metric(name: "Evidenced (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Evidenced_GBP: metric(name: "Evidenced (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      CTA_Evidenced_FTE: metric(name: "CTA Evidenced (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      CTA_Evidenced_GBP: metric(name: "CTA Evidenced (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Outturn_GBP_run_rate: metric(name: "Outturn (£) run rate") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Cost_to_deliver_GBP: metric(name: "Cost to deliver (£)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Weighted_Pipeline_Cost_FTE: metric(name: "Weighted Pipeline Cost (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Net_Cash_Impact_GBP: metric(name: "Net Cash Impact (£) (outflow -ve / inflow +ve)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                      Unweighted_Pipeline_Cost_FTE: metric(name: "Unweighted Pipeline Cost (# FTE)") {
                        Actuals: values(dates: ["2019-01-01", "2019-02-01", "2019-03-01", "2019-04-01", "2019-05-01", "2019-06-01", "2019-07-01", "2019-08-01", "2019-09-01", "2019-10-01", "2019-11-01", "2019-12-01"], dataset: "actual")
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}'''


# In[103]:


def getTokens(basics):
    rurl= "https://eu.shibumi.com/api/oauth2/token"
    data = {"grant_type":"password","username":basics["username"],"password":basics["password"],"client_id":basics["client_id"],"client_secret":basics["client_secret"]}
    response = requests.post(rurl,params=data)
    if response.status_code in[200]:
        respjson  = response.json()
        return respjson
    else:
        return(response.text)


# In[104]:


def getdata(url,query,header):
    respdata = requests.post(url,json={'query': query}, headers=headers)
    return respdata.json()


# In[105]:


token = getTokens(basics)


# In[106]:


headers = {
    'Authorization': "Bearer "+ token["access_token"],
    'Content-Type': "application/json",
    'Host': "eu.shibumi.com",
    'Accept-Encoding': "gzip, deflate",
    'Content-Length': str(len(query)),
    'Connection': "keep-alive",
    'cache-control': "no-cache"
    }


# In[107]:


result  =  getdata(url,query,headers)


# In[108]:


data_n = json_normalize(result["data"]["workItems"]["Programme"]["descendants"]["Division"],["descendants","BU_Level_4","descendants","BU_Level_5","descendants","Initiative"],meta = ["DivisionName",["descendants","BU_Level_4","BU_Level_4Name"],["descendants","BU_Level_4","descendants","BU_Level_5","BU_Level_5Name"]],sep='_')


# In[109]:


df = pd.DataFrame(data_n)


# In[110]:


df = df.set_index("Initiative_ID")


# In[111]:


initiatives = df[['name',
                  'Comments__c', 
                  'Cost_centre__c', 
                  'Cost_package__c',
                  'Current_RAG_status__c', 
                  'Evidence_Status__c', 
                  'Initiative_current_addressable_base_FTE__c',
                  'Initiative_current_addressable_base__c',
                  'Initiative_interdependencies__c', 
                  'Initiative_sponsor__c',
                  'Lever__c', 
                  'Savings_Measure__c', 
                  'Sub_lever__c',
                  'Sub_lever_if_Other_please_specify__c', 
                  'Target_FTE',
                  'Target_cost_savings_issued_to_the_initia__c',
                  'Target_cost_to_achieve_given_to_the_init__c',
                  'Workstream__c',
                  'description', 
                  'DivisionName', 
                  'descendants_BU_Level_4_BU_Level_4Name',
                  'descendants_BU_Level_4_descendants_BU_Level_5_BU_Level_5Name']]


# In[112]:


initiatives.to_csv("initiatives.csv")


# In[113]:


metrics = df[['Weighted_Pipeline_Saving_FTE',
              'CTA_2018_Run_rate_GBP',
              'Weighted_Pipeline_GBP',
              'CTA_2019_Run_rate_GBP',
              'Severance_costs_GBP',
              'Offshore_transition_costs_GBP',
              'Other_costs_to_achieve_GBP',
              'Central_Programme_costs_GBP',
              'Invest_to_Save_GBP',
              'Unweighted_Pipeline_Saving_FTE',
              'Unweighted_Pipeline_GBP',
              'Validated_FTE',
              'Validated_GBP',
              'Actioned_FTE',
              'Actioned_GBP', 
              'Evidenced_FTE',
              'Evidenced_GBP',
              'CTA_Evidenced_FTE',
              'CTA_Evidenced_GBP',
              'Outturn_GBP_run_rate',
              'Cost_to_deliver_GBP',
              'Weighted_Pipeline_Cost_FTE',
              'Net_Cash_Impact_GBP',
              'Unweighted_Pipeline_Cost_FTE']].stack().apply(pd.Series).reset_index()


# In[114]:


metrics = metrics.set_index("Initiative_ID")


# In[115]:


metricslong = metrics["Actuals"].apply(pd.Series)
metricslong.columns = ["Jan_2019", "Feb_2019", "Mar_2019", "Apr_2019", "May_2019", "Jun_2019", "Jul_2019", "Aug_2019", "Sep_2019", "Oct_2019", "Nov_2019", "Dec_2019"]
metricslong["DataSet"] = "Actuals"
metricslong["Name"] = metrics["level_1"]


# In[116]:


final = initiatives.join(metricslong)


# In[117]:


final = final.rename(columns= {'name':'Initiative Name', 
                       'Comments__c':'Comments', 
                       'Cost_centre__c':'Cost centre', 
                       'Cost_package__c':'Cost package',
                       'Current_RAG_status__c':'Current RAG status',
                       'Evidence_Status__c':'Evidence Status',
                       'Initiative_current_addressable_base_FTE__c':'Initiative current addressable base (# FTE)',
                       'Initiative_current_addressable_base__c':'Initiative current addressable base (£)',
                       'Initiative_interdependencies__c':'Initiative interdependencies',
                       'Initiative_sponsor__c':'Initiative sponsor',
                       'Lever__c':'Lever',
                       'Savings_Measure__c':'Savings Measure',
                       'Sub_lever__c':'Sub-lever',
                       'Sub_lever_if_Other_please_specify__c':'Sub-lever - if Other please specify', 
                       'Target_FTE':'Target cost savings aimed at through this initiative (# FTE)', 
                       'Target_cost_savings_issued_to_the_initia__c':'Target cost savings aimed at through this initiative (£)',
                       'Target_cost_to_achieve_given_to_the_init__c':'Target cost to achieve budget given to the initiative owner (£)', 
                       'Workstream__c':'Workstream',
                       'description':'Description',
                       'DivisionName':'Division', 
                       'descendants_BU_Level_4_BU_Level_4Name':'BU (Level 4)',
                       'descendants_BU_Level_4_descendants_BU_Level_5_BU_Level_5Name':'BU (Level 5)',
                       'Jan_2019':'Jan_2019', 
                       'Feb_2019':'Feb_2019', 
                       'Mar_2019':'Mar_2019',
                       'Apr_2019':'Apr_2019',
                       'May_2019':'May_2019', 
                       'Jun_2019':'Jun_2019',
                       'Jul_2019':'Jul_2019',
                       'Aug_2019':'Aug_2019',
                       'Sep_2019':'Sep_2019', 
                       'Oct_2019':'Oct_2019',
                       'Nov_2019':'Nov_2019',
                       'Dec_2019':'Dec_2019',
                       'DataSet':'Data Set',
                       'Name':'Name' })


# In[118]:


final = final.reset_index()


# In[119]:


print(final)


# In[34]:


final.to_csv("Result.csv")


# In[ ]:


engine = sa.create_engine('mssql+pyodbc://@' + 'WOKX3RN1V' + '/' + 'X3RN' + '?driver=ODBC+Driver+13+for+SQL+Server')
final.to_sql('CTT_raw_AAG', con=engine, if_exists='replace')

