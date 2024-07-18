# Connecting to the database

The customer of this research is a large telecommunications company that provides services throughout the CIS. The company is faced with the task of determining the current level of consumer loyalty, or NPS (from the English Net Promoter Score), among customers from Russia.
To determine the level of loyalty, customers were asked a classic question: “On a scale of 1 to 10, rate the likelihood that you would recommend the company to friends and acquaintances.”
The company conducted a survey and asked me to prepare a dashboard with its results. They did not deploy a large database for such a task and uploaded the data to SQLite.


```python
# import the libraries necessary for further work

import os
import pandas as pd
import numpy as np

from sqlalchemy import create_engine
```


```python
# paths to potential database locations
path_to_db_local = 'telecomm_csi.db'
path_to_db_platform = '/datasets/telecomm_csi.db'
path_to_db = None

# check if the local database exists; if not, check the platform database
if os.path.exists(path_to_db_local):
    path_to_db = path_to_db_local  # use local database if available
elif os.path.exists(path_to_db_platform):
    path_to_db = path_to_db_platform  # fallback to platform database if local is absent
else:
    # raise an exception if no database is found at either location
    raise Exception('Файл с базой данных SQLite не найден!')

# initialize database connection if a valid database path was determined
if path_to_db:
    engine = create_engine(f'sqlite:///{path_to_db}', echo=False)  # create SQLAlchemy engine without logging

```

# Loading data


```python
query = \
"""
-- define a common table expression (CTE) named ExtendedUser
WITH ExtendedUser AS (
  SELECT
    *,
    -- determine if the user is new based on the 'lt_day' field
    CASE
      WHEN lt_day >= 365 THEN 'no'
      ELSE 'yes'
    END as is_new,
    -- classify users into NPS groups based on their 'nps_score'
    CASE
      WHEN nps_score <= 6 THEN 'detractors'
      WHEN nps_score BETWEEN 7 AND 8 THEN 'passives'
      WHEN nps_score >= 9 THEN 'promoters'
    END as nps_group,
    -- convert 'gender_segment' numerical codes to readable text
    CASE
      WHEN gender_segment == 1 THEN 'female'
      ELSE 'male'
    END as gender_segment_w
  FROM
    user
)

-- main SELECT query to retrieve user data along with related dimensional data
SELECT
  u.user_id,
  u.lt_day,
  u.is_new,
  u.age,
  u.gender_segment_w,
  u.os_name,
  u.cpe_type_name,
  l.country,
  l.city,
  ags.title as age_segment,
  ts.title as traffic_segment,
  ls.title as lifetime_segment,
  u.nps_score,
  u.nps_group
FROM
  ExtendedUser as u  -- use the CTE defined earlier
  JOIN location as l ON u.location_id = l.location_id 
  JOIN age_segment as ags ON u.age_gr_id = ags.age_gr_id  
  JOIN traffic_segment as ts ON u.tr_gr_id = ts.tr_gr_id  
  JOIN lifetime_segment as ls ON u.lt_gr_id = ls.lt_gr_id;  

"""
```


```python
# check 
df = pd.read_sql(query, engine)
df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>lt_day</th>
      <th>is_new</th>
      <th>age</th>
      <th>gender_segment_w</th>
      <th>os_name</th>
      <th>cpe_type_name</th>
      <th>country</th>
      <th>city</th>
      <th>age_segment</th>
      <th>traffic_segment</th>
      <th>lifetime_segment</th>
      <th>nps_score</th>
      <th>nps_group</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A001A2</td>
      <td>2320</td>
      <td>no</td>
      <td>45.0</td>
      <td>female</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Уфа</td>
      <td>05 45-54</td>
      <td>04 1-5</td>
      <td>08 36+</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>1</th>
      <td>A001WF</td>
      <td>2344</td>
      <td>no</td>
      <td>53.0</td>
      <td>male</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Киров</td>
      <td>05 45-54</td>
      <td>04 1-5</td>
      <td>08 36+</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>2</th>
      <td>A003Q7</td>
      <td>467</td>
      <td>no</td>
      <td>57.0</td>
      <td>male</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Москва</td>
      <td>06 55-64</td>
      <td>08 20-25</td>
      <td>06 13-24</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>3</th>
      <td>A004TB</td>
      <td>4190</td>
      <td>no</td>
      <td>44.0</td>
      <td>female</td>
      <td>IOS</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>РостовнаДону</td>
      <td>04 35-44</td>
      <td>03 0.1-1</td>
      <td>08 36+</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>4</th>
      <td>A004XT</td>
      <td>1163</td>
      <td>no</td>
      <td>24.0</td>
      <td>male</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Рязань</td>
      <td>02 16-24</td>
      <td>05 5-10</td>
      <td>08 36+</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>5</th>
      <td>A005O0</td>
      <td>5501</td>
      <td>no</td>
      <td>42.0</td>
      <td>female</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Омск</td>
      <td>04 35-44</td>
      <td>05 5-10</td>
      <td>08 36+</td>
      <td>6</td>
      <td>detractors</td>
    </tr>
    <tr>
      <th>6</th>
      <td>A0061R</td>
      <td>1236</td>
      <td>no</td>
      <td>45.0</td>
      <td>male</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Уфа</td>
      <td>05 45-54</td>
      <td>06 10-15</td>
      <td>08 36+</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>7</th>
      <td>A009KS</td>
      <td>313</td>
      <td>yes</td>
      <td>35.0</td>
      <td>male</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Москва</td>
      <td>04 35-44</td>
      <td>13 45-50</td>
      <td>05 7-12</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>8</th>
      <td>A00AES</td>
      <td>3238</td>
      <td>no</td>
      <td>36.0</td>
      <td>female</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>СанктПетербург</td>
      <td>04 35-44</td>
      <td>04 1-5</td>
      <td>08 36+</td>
      <td>10</td>
      <td>promoters</td>
    </tr>
    <tr>
      <th>9</th>
      <td>A00F70</td>
      <td>4479</td>
      <td>no</td>
      <td>54.0</td>
      <td>female</td>
      <td>ANDROID</td>
      <td>SMARTPHONE</td>
      <td>Россия</td>
      <td>Волгоград</td>
      <td>05 45-54</td>
      <td>07 15-20</td>
      <td>08 36+</td>
      <td>9</td>
      <td>promoters</td>
    </tr>
  </tbody>
</table>
</div>




```python
# exporting as csv to continue work in data-viz
df.to_csv('telecomm_csi_tableau.csv', index=False)
```

# Tableau

All other work related to analytics and explication of conclusions and insights was done in Tableau.

The project is presented in the form of a story. Each slide presents one specific topic and is accompanied by conclusions and observations. There are three slides in total: about the history of the speeches, about the topics of the speeches and about the authors of the speeches. Each slide is a separate dashboard with active action elements.

Data-viz is [here](https://public.tableau.com/app/profile/artem.aglyamov/viz/projectNo_2TELECOMNPS/Story1 "yep, that's right, tap on me")
