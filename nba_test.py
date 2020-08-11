#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as p


# In[ ]:





# In[29]:


players = p.read_json('players.json')
league = p.read_json('league.json')


# In[3]:


players.size


# In[4]:


players.describe()


# In[9]:


players_df = players['league']


# In[10]:


nba_players = players_df['standard']


# In[ ]:





# In[12]:


nba_players


# In[14]:


nba_df = p.DataFrame(nba_players)


# In[15]:


nba_df.describe()


# In[16]:


nba_df.head()


# In[17]:


nba_df.describe()


# In[22]:


nba_df.columns


# In[25]:


nba_df['heightFeet'].unique()


# In[28]:


nba_df.groupby('heightFeet').count()


# In[32]:


league = league['league']


# In[34]:


nba = league['standard']


# In[36]:


nba_df = p.DataFrame(nba)


# In[38]:





# In[ ]:




