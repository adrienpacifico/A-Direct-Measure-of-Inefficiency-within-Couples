
# coding: utf-8

from __future__ import division
import pandas as pd ; pd.set_option("display.max_columns",200)

# # Inverstigation of EDP

# Taken originaly from Table Find concubin + biologic child fiscal household
# Data made in Isolate_concubin_with_child_under_majority

# In[1]:

# In[2]:

import matplotlib.pyplot as plt
import pickle as pickle

# In[3]:

income_year = 2014

# In[4]:

store_path = './Data/hdf/edp_2015_final.h5'
Store = pd.HDFStore(store_path)


# In[5]:

Store





# In[7]:



def create_data_year(income_year = 2014):
    income_year = income_year




    def load_fisc_i_by_year(year = income_year):
        df_fisci = pd.read_hdf(store_path, 'FISC_INDIVIDU_{}'.format(year))
        return df_fisci
    def load_fisc_r_by_year(year = income_year):
        df_fiscr = pd.read_hdf(store_path, 'FISC_REVENU_{}'.format(year))
        return df_fiscr
    def load_fisc_l_by_year(year = income_year):
        df_fiscr = pd.read_hdf(store_path, 'FISC_LOGEMENT_{}'.format(year))
        return df_fiscr
    
    def load_df_fiscrevdet_by_year(year = income_year+1):
        df_fiscrevdet = pd.read_hdf(store_path, 'FISC_REVDET_{}'.format(year))
        return df_fiscrevdet



    # In[9]:
    
    df_fisci = load_fisc_i_by_year(year = income_year)
    print df_fisci.shape
        
    
    import pickle
    keep = pickle.load(
                open(u"./Programme/Pickle/final_select_rev_{}.p".format(year), 'rb'))
    
    df_fisci = df_fisci[df_fisci.ID_FISC_LOG_DIFF.isin(keep)]
    

    
    
    # In[9]:
    
    fisci_id_diff = df_fisci.ID_DIFF
    
    
    # #### TODO:  problème on a des individus qui ont deux identifiants fiscal individuel.
    
    # In[10]:
    
    df_descendance = pd.read_hdf(store_path,"DESCENDANCE")
    
    
    # In[11]:
    
    df_descendance.head()
    
    
    # In[ ]:
    
    
    is_numeric = df_descendance.ID_DIFF.str.isnumeric()
    df_descendance = df_descendance.query("@is_numeric == True")
    df_descendance["ID_DIFF"] = df_descendance.ID_DIFF.astype("float")
    
    # In[12]:
    
    descendance_ID_DIFF = df_descendance.ID_DIFF.unique()
    
    
    # In[ ]:
    
    
    
    
    # In[13]:
    
    df_fisci['IS_IN_DESCENDANCE'] = df_fisci.ID_DIFF.isin(descendance_ID_DIFF).astype('int')
    
    
    # In[14]:
    
    df_fisci.groupby('ID_FISC_LOG_DIFF').sum()['IS_IN_DESCENDANCE'].value_counts()
    
    
    # In[15]:
    
    
    
    
    # 87000 foyers avec au moins un individu EDP dans table naissance ou descendance.
    
    # ### Voir si enfants ou parents
    
    # Descendance, 90680 parents, 33 personnes à charges
    
    # In[16]:
    
    df_fisci[df_fisci.IS_IN_DESCENDANCE.astype('bool')].TYPE_FISC.value_counts()
    
    
    # In[17]:
    
    df_fisci_etat_c = df_fisci[df_fisci.IS_IN_DESCENDANCE.astype('bool')]
    
    
    # In[18]:
    
    df_fisci_etat_c.shape
    
    
    # In[19]:
    
    id_diff_fisci_c = df_fisci_etat_c.ID_DIFF
    
    
    # Nombre de naissances repérées par ID_DIFF
    
    # In[20]:
    
    df_descendance[
        df_descendance.ID_DIFF.isin(id_diff_fisci_c)
               ].groupby('ID_DIFF').count()[ 'ID_EVT_DIFF'].value_counts()
    
    
    # 45755 individus ont une naissance renseignée, 34000 en ont deux, 7374 en ont trois, 1393 en ont 4.
    
    # ##### Virer les observations contenant des enfants nais sans vie
    
    # In[21]:
    
    df_descendance.EVT_TYPE.value_counts()
    
    
    # In[22]:
    
    df_descendance = df_descendance[~(df_descendance.EVT_TYPE == 'ESV')] #On gagne 269 ménages qui ont le même nombre d'enfants en faisant ça
    # On perd 377 ménages qui ont le nombre d'enfant inférieur au nombre de naissance.
    # On gagne 66 ménages qui ont le nombre d'enfant supérieur au nombre de naissance.
    
    
    # In[23]:
    
    #df_descendance.EVT_TYPE.value_counts()
    
    
    # In[ ]:
    
    
    
    
    # In[24]:
    
    df_nb_naissance = df_descendance[df_descendance.ID_DIFF.isin(id_diff_fisci_c)
                                    ].groupby('ID_DIFF').count().reset_index()[['ID_DIFF','EVT_TYPE']]
    df_nb_naissance =  df_nb_naissance.rename(columns = {'EVT_TYPE' : 'NB_NAISSANCE'})
    df_nb_naissance.head()
    
    
    # In[25]:
    
    nb_indiv_in_household = df_fisci.groupby('ID_FISC_LOG_DIFF' ).count()
    nb_indiv_in_household['ID_DIFF']  = df_fisci.groupby('ID_FISC_LOG_DIFF' ).first()
    
    
    # In[26]:
    
    nb_indiv_in_household = nb_indiv_in_household.reset_index()[['ID_FISC_LOG_DIFF','ID_DIFF','AN_FISC']]
    
    
    # In[27]:
    
    nb_indiv_in_household['NB_CHILD_IN_HOUSEHOLD'] = nb_indiv_in_household['AN_FISC'] - 2 
    
    
    # In[28]:
    
    nb_indiv_in_household.drop('AN_FISC',1, inplace=True)
    
    
    # In[29]:
    
    merge = nb_indiv_in_household.merge(df_nb_naissance, on = 'ID_DIFF');merge.head()
    
    
    # In[ ]:
    
    
    
    
    # In[30]:
    
    print (merge.NB_CHILD_IN_HOUSEHOLD == merge.NB_NAISSANCE).value_counts()
    print (merge.NB_CHILD_IN_HOUSEHOLD == merge.NB_NAISSANCE).value_counts(normalize = True)
    
    
    # 36% des ménages n'ont pas le même nombre de naissance dans la base naissance et le même nombre d'enfants dans le logement fiscal.
    # Pourquoi ? Sous déclaration ?
    
    # In[31]:
    
    8532 - 8909
    
    
    # In[32]:
    
    (merge.NB_CHILD_IN_HOUSEHOLD < merge.NB_NAISSANCE).value_counts()
    
    
    # Hypothèses : enfants morts, enfant parti dans autre foyer, trou de déclaration (trou de 1982 à 1997)
    
    
    
    # In[34]:
    
    (merge.NB_CHILD_IN_HOUSEHOLD > merge.NB_NAISSANCE).value_counts()
    
    
    # In[35]:
    
    df_fisci[df_fisci.TYPE_FISC == '1'].JNAIS.isnull().value_counts()
    
    
    # Jours de naissance renseigné pour tout les parents
    
    # In[36]:
    
    df_fisci.T_CHARGE.value_counts()
    
    
    # T_CHARGE : Code situation des personnes à charge. F : normal. G: Enfant à charge titulaire de la carte d'invalidité.
    # 
    # On a pas d'enfant en garde alternée dans le sample (bon signal).
    
    # In[ ]:
    
    
    
    
    # ## Début Zap 1
    
    # In[37]:
    
    df_descendance[~df_descendance.CTX_NAV_PREC_DATE.isnull()].ENF_IND_NAI_DATE.str[:4].value_counts().head()
    
    
    # In[38]:
    
    hello = df_descendance[df_descendance.ID_DIFF.isin(id_diff_fisci_c)
                  ].groupby('ID_DIFF').CTX_NAV_PREC_DATE.apply(list)
    
    
    # In[39]:
    
    hello.shape
    
    
    # In[40]:
    
    hello.head()
    
    
    # In[ ]:
    
    
    
    
    # On garde dans la table descendance que les observations ou :
    # - l'id_diff est dans fisci
    # - 
    
    # In[41]:
    
    df = df_descendance[
        df_descendance.ID_DIFF.isin(id_diff_fisci_c) #dans fisci
                  ].groupby('ID_DIFF').first().reset_index()
    
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # In[42]:
    
    hello = pd.DataFrame(hello).reset_index();hello.head()
    
    
    # In[43]:
    
    df = df.merge(hello,on='ID_DIFF')
    
    
    # In[44]:
    
    df.columns
    
    
    # In[ ]:
    
    
    
    
    # In[45]:
    
    df = df[[  u'CTX_NAV_PREC_DATE_y','CTX_ACC_ENF_NBR','CTX_MERE_VIVANT_ENF_PREC_NBR',
        u'ID_DIFF', u'ID_EVT_DIFF', u'EVT_TYPE', u'OPE_TYPE', u'ENF_IND_SEXE',
           u'ENF_IND_NAI_DATE', u'ENF_IND_NAI_LIEU_DEPCOM', u'PERE_IND_NAI_DATE',
           u'MERE_IND_NAI_DATE', u'PERE_IND_NAI_LIEU_DEPCOM',
           u'MERE_IND_NAI_LIEU_DEPCOM', u'PERE_ADR_LIEU_DEPCOM',
           u'MERE_ADR_LIEU_DEPCOM', u'ADRDOMPAR', u'PERE_NAT_CODE',
           u'MERE_NAT_CODE', u'PERE_CS_CODE', u'MERE_CS_CODE', u'MAR_DATE',
           u'MAR_LIEU_DEPCOM', u'ORIGINOM', u'ENF_GEMEL_INDIC',
           u'NB_EDP',
           u'CTX_CATACC', u'CTX_ACC_ENF_NBR', u'I_ENF_IND_NAI_DATE',
           u'I_ENF_IND_SEXE', u'I_MAR_DATE', u'I_MAR_LIEU_DEPCOM',
           u'I_MERE_ADR_LIEU_DEPCOM', u'I_MERE_IND_NAI_DATE',
           u'I_MERE_IND_NAI_LIEU_DEPCOM', u'I_MERE_NAT_CODE', u'I_MERE_CS_CODE',
           u'I_PERE_ADR_LIEU_DEPCOM', u'I_PERE_IND_NAI_DATE',
           u'I_PERE_IND_NAI_LIEU_DEPCOM', u'I_PERE_NAT_CODE', u'I_PERE_CS_CODE',
           ]]
    
    
    # In[46]:
    
    df.head(5)
    
    
    # Technique
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # Question : pourquoi le premier CTX_NAV_Prec_date pas toujours nan ou 0000 ?
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # In[ ]:
    
    
    
    
    # In[47]:
    
    df.CTX_MERE_VIVANT_ENF_PREC_NBR.value_counts(dropna = False).head()
    
    
    # In[48]:
    
    df[['OPE_TYPE','CTX_MERE_VIVANT_ENF_PREC_NBR' ]].head()
    
    
    # In[49]:
    
    import numpy as np
    
    
    # In[ ]:
    
    
    
    
    # In[50]:
    
    df[['OPE_TYPE','CTX_MERE_VIVANT_ENF_PREC_NBR' 
       ]].pivot_table( columns = ['CTX_MERE_VIVANT_ENF_PREC_NBR'], 
                      index=['OPE_TYPE'],
                      aggfunc = len, fill_value=0)
    
    
    # In[51]:
    
    df2  = df[['OPE_TYPE','CTX_MERE_VIVANT_ENF_PREC_NBR' ]]
    
    
    # In[52]:
    
    df2[(df2.CTX_MERE_VIVANT_ENF_PREC_NBR == '*')&(df2.OPE_TYPE == 'MERE' )].shape
    
    
    # In[53]:
    
    df2.loc[ df2.CTX_MERE_VIVANT_ENF_PREC_NBR == '*', 'CTX_MERE_VIVANT_ENF_PREC_NBR'] = 0
    
    
    # In[54]:
    
    df2.loc[ df2.CTX_MERE_VIVANT_ENF_PREC_NBR.isnull(), 'CTX_MERE_VIVANT_ENF_PREC_NBR'] = 0
    
    
    # In[55]:
    
    df2['CTX_MERE_VIVANT_ENF_PREC_NBR'] = df2.CTX_MERE_VIVANT_ENF_PREC_NBR.astype('int32')
    
    
    # In[56]:
    
    df2.head()
    
    
    # In[57]:
    
    df2[['OPE_TYPE','CTX_MERE_VIVANT_ENF_PREC_NBR' 
       ]].pivot_table( columns = ['CTX_MERE_VIVANT_ENF_PREC_NBR'], 
                      index=['OPE_TYPE'],
                      aggfunc = len, fill_value=0)
    
    
    # In[58]:
    
    df['NB_CTX_NAV_PREC_DATE'] = df.CTX_NAV_PREC_DATE_y.apply(lambda x: len(x))
    
    
    # In[59]:
    
    df[['NB_CTX_NAV_PREC_DATE', u'CTX_NAV_PREC_DATE_y','CTX_ACC_ENF_NBR','CTX_MERE_VIVANT_ENF_PREC_NBR',u'OPE_TYPE',
        u'ID_DIFF', u'ID_EVT_DIFF', u'EVT_TYPE',  u'ENF_IND_SEXE',
           u'ENF_IND_NAI_DATE']].head()
    
    
    # TODO : investiguer un peu plus les rangs de naissance, mais ça semble assez peu prometteur...
    
    # ## Fin Zap 1
    
    # ### Regarder les dates de naissances parents/enfants
    
    # Stratégie :
    # 
    # Si enfant EDP --> 2 bdates naissance parents --> ok paternité. Par contre difficile de déterminer pour frere et soeurs.
    # 
    # Si non EDP & 1 des deux parents EDP -- > 2 dates naissances parents --> ok !
    
    # In[ ]:
    
    
    
    
    # On prends que les parents qui sont EDP.
    
    # In[60]:
    
    df_fisci_declar_edp = df_fisci[(df_fisci.TYPE_FISC=='1')&
                  (~df_fisci.ID_DIFF.isnull())]
    
    
    # In[61]:
    
    df_fisci_declar_edp.shape
    
    
    # In[62]:
    
    df_fisci_declar_edp.ID_DIFF.nunique()
    
    
    # ### Attention on a des individus en double.
    
    # TODO : les enlever, par contre, si on les enlèves à la bourrin on peut casser la structure du ménage...
    
    # In[ ]:
    
    
    
    
    # In[63]:
    
    id_diff = df_fisci_declar_edp.ID_DIFF
    
    
    # In[64]:
    
    df_descendance[df_descendance.ID_DIFF.isin(id_diff)].ID_DIFF.unique().shape
    
    
    # Parmis les 115367 parents EDP, seul 89520 figurent dans la base descendance, soit 77% .
    # Pourquoi ? Parents non biologiques ?
    
    # On va vérifier que les dates de naissance des parents sont les mêmes que les enfants.
    
    # In[65]:
    
    df_descendance_select = df_descendance[df_descendance.ID_DIFF.isin(id_diff)] #Prends que les parends edp
    
    #♣Transforme les dates de naissances en format date
    # In[66]:
    
    df_descendance_select['PERE_IND_NAI_DATE'] = pd.to_datetime(
        df_descendance_select['PERE_IND_NAI_DATE'], infer_datetime_format=True, coerce=True
        )
    df_descendance_select['MERE_IND_NAI_DATE'] = pd.to_datetime(
        df_descendance_select['MERE_IND_NAI_DATE'], infer_datetime_format=True, coerce=True
        )
        
    df_descendance_select['ENF_IND_NAI_DATE'] = pd.to_datetime(
        df_descendance_select['ENF_IND_NAI_DATE'], infer_datetime_format=True, coerce=True
        )
    
    
    # In[ ]:
    
    
    
    
    # In[67]:
    
    print df_descendance_select.PERE_IND_NAI_DATE.isnull().value_counts()
    print df_descendance_select.MERE_IND_NAI_DATE.isnull().value_counts()
    print df_descendance_select.ENF_IND_NAI_DATE.isnull().value_counts()
    print df_descendance_select.PERE_IND_NAI_DATE.isnull().value_counts(normalize = True)
    print df_descendance_select.MERE_IND_NAI_DATE.isnull().value_counts(normalize = True)
    print df_descendance_select.ENF_IND_NAI_DATE.isnull().value_counts(normalize = True)
    
    
    # 3%  des pères et 0.3% des mères n'ont pas la date de naissance renseignée, pourquoi ? TODO.
    
    # On vire tout les logement fiscaux dont un enfant d'un ménage 'parent EDP' n'a pas la date du père ou de la mère renseignée.
    
    # In[68]:
    
    select_id_diff = df_descendance_select[(df_descendance_select.PERE_IND_NAI_DATE.isnull())|
                                          (df_descendance_select.MERE_IND_NAI_DATE.isnull())|
                                           (df_descendance_select.ENF_IND_NAI_DATE.isnull())
                                           ].ID_DIFF
    
    
    # In[69]:
    
    temp_df = df_fisci_declar_edp[df_fisci_declar_edp.ID_DIFF.isin(select_id_diff)].groupby('ID_FISC_LOG_DIFF').count()
    
    
    # In[70]:
    
    temp_df.reset_index(inplace = True)
    
    
    # In[71]:
    
    to_drop_id_fisc_log = temp_df.ID_FISC_LOG_DIFF
    
    
    # In[72]:
    
    df_fisci_declar_date_parents = df_fisci_declar_edp[~df_fisci_declar_edp.ID_FISC_LOG_DIFF.isin(to_drop_id_fisc_log)] #Vire les 
    #logements fiscaux dont la date d'un des deux parent (ou de l'enfant) n'est pas renseigne
    
    
    # In[73]:
    
    df_fisci_declar_date_parents.head()
    
    
    # A faire : voir si on a la même date de naissance des enfants
    
    # df_select_2 : on garde que les ménages ou la date des parents et des enfants est renseigné dans l'état civil (fille de df_select ou on ne gardait que les déclarants EDP de fisci qui étaient dans DESCENDANCE 89000 individus.)
    
    # In[74]:
    
    df_descendance_select_2 = df_descendance[
        df_descendance.ID_DIFF.isin(df_fisci_declar_date_parents.ID_DIFF)]
    
    
    # In[75]:
    
    df_descendance_select_2.shape
    
    
    # In[76]:
    
    df_descendance_select_2.ENF_IND_NAI_DATE.str[:4].value_counts().head(12)
    
    
    # En 2004 les jours EDP on été élargi de 4 à 16. Univquement les naissance d'après 2004 rentre en compte pour ces 12/16 de la pop.
    # (D'où absence d'enfants dans de nombreux foyers).
    # 
    
    # In[77]:
    
    df_descendance_select_2[
        df_descendance_select_2.ENF_IND_NAI_DATE.str[:4]=='2015'].ENF_IND_NAI_DATE.str[5:7].value_counts()
    
    
    # In[78]:
    
    df_descendance.ENF_IND_NAI_DATE.str[:4].value_counts().head(13)
    
    
    # On a pas le même nopmbre de naissance en fonction de l'année. Pourquoi ?
    
    # Lorsque l'EDP a été élargi à de nouveaux jours, l'EDP a débuté les trajectoires des nouveaux arrivants par le premier événement reçu par l'état civil ou le recensement. Leur
    # trajectoire n'a pas été complétée par les événements antérieurs à la date de leur entrée dans l'EDP. Notamment, on ne dispose pas pour eux des bulletins de naissance de
    # leurs enfants antérieurs à leur entrée dans l'EDP.
    
    # TODO: remarques au dessus.
    
    # In[79]:
    
    df_descendance_select_2.ENF_IND_NAI_DATE.str[0:4].value_counts().head(11)
    
    
    # On a pas le meme nombre pas an car les logements fiscaux qui existent en 2015 n'existaient pas forcément avant.
    # Les nouveaux logements fiscaux sont plus jeune, et ont donc des enfants qui sont plus jeune.
    # Normalement pas de biais de sélection.
    
    # ### Regarder si on a la même année de naissance pour tout les enfants + voir si date de naissance des parents est la même.
    
    # In[80]:
    
    df_descendance_select_2.groupby('ID_DIFF').ID_DIFF.count().head()
    
    
    # In[81]:
    
    df_descendance_select_2.groupby('ID_DIFF').ID_DIFF.count().value_counts()
    
    
    # In[82]:
    
    df_nb_descendant_dans_descendance_by_id_diff = pd.DataFrame(df_descendance_select_2.groupby('ID_DIFF').ID_DIFF.count())
    df_nb_descendant_dans_descendance_by_id_diff = df_nb_descendant_dans_descendance_by_id_diff.rename(
                                columns = {'ID_DIFF' : 'NB_NAISSANCE'}).reset_index()
    
    
    # In[83]:
    
    #pd.merge(df_fisci,df_nombre_descendant_by_id_diff, on = 'ID_DIFF')
    
    
    # In[84]:
    
    #(df_descendance_select_2.PERE_IND_NAI_DATE == ANAIS + MNAIS+JNAIS) &(fisci.sexe == 1)
    
    
    # On crée une variable date de naissance à partir de l'année du mois et du jour.
    
    # In[85]:
    
    date_naissance = pd.to_datetime((df_fisci.ANAIS.apply(
                                        lambda x: x.astype('str').replace('.0',''))+'-'+
                                      df_fisci.MNAIS.apply(
                                        lambda x: x.astype('str').replace('.0',''))+'-'+
                                      df_fisci.JNAIS.apply(
                                        lambda x: x.astype('str').replace('.0',''))), 
                                                   coerce=True, infer_datetime_format=True)
    # In[86]:
    
    df_fisci['Date_Naissance'] = date_naissance
    df_fisci_date_naissance = df_fisci
    
    
    # In[87]:
    
    df_fisci_date_naissance['SEXE'
        ]=df_fisci_date_naissance.SEXE.astype('float')
    
    
    # In[ ]:
    
    
    
    
    # In[88]:
    
    ((df_fisci_date_naissance.SEXE == 1) & 
     (df_fisci_date_naissance.TYPE_FISC == '1')).value_counts()
     
    
    
    # On a 180090 père déclarant dans la table fisci_final
    
    # In[89]:
    
    ((df_fisci_date_naissance.SEXE == 2) & 
     (df_fisci_date_naissance.TYPE_FISC == '1')).value_counts()
    
    
    # Et  180518 mère délcante 
    
    # In[90]:
    
    df_fisci_date_naissance[((df_fisci_date_naissance.SEXE == 1) & 
     (df_fisci_date_naissance.TYPE_FISC == '1'))].Date_Naissance.isnull().value_counts()
    
    
    # TODO : Gerer les 31 gus qui ont pas leur date de naissance renseignée
    
    # In[ ]:
    
    
    
    
    # On met la date de naissance du père et de la mère dans fisci
    
    # In[91]:
    
    df_fisci_date_naissance['Date_Naissance_Pere_fisci'] = df_fisci_date_naissance[((df_fisci_date_naissance.SEXE == 1) & 
     (df_fisci_date_naissance.TYPE_FISC == '1'))].Date_Naissance
    df_fisci_date_naissance['Date_Naissance_Mere_fisci'] = df_fisci_date_naissance[((df_fisci_date_naissance.SEXE == 2) & 
     (df_fisci_date_naissance.TYPE_FISC == '1'))].Date_Naissance
    
    
    # In[92]:
    
    df_fisci_date_naissance[['ID_FISC_LOG_DIFF','Date_Naissance_Pere_fisci', 'Date_Naissance_Mere_fisci',
                                   'TYPE_FISC', 'ANAIS', 'SEXE']].head()
    
    
    # On met ensuite la date de naissance des parents de chaque logement fiscal.
    
    # In[93]:
    
    grpby_pere = df_fisci_date_naissance.sort('Date_Naissance_Pere_fisci').groupby('ID_FISC_LOG_DIFF').first(
        )['Date_Naissance_Pere_fisci']
    
    
    # In[94]:
    
    grpby_mere = df_fisci_date_naissance.sort('Date_Naissance_Mere_fisci').groupby('ID_FISC_LOG_DIFF').first(
        )['Date_Naissance_Mere_fisci']
    
    
    # In[95]:
    
    df_date_naissance = pd.merge(pd.DataFrame(grpby_pere).reset_index(), pd.DataFrame(grpby_mere).reset_index(),
             on='ID_FISC_LOG_DIFF')
    
    
    # In[96]:
    
    df_date_naissance.head()
    
    
    # On merge a fici_final pour avoir la date de naissance des deux déclarants dans chaque fisci final
    
    # In[97]:
    
    #verifier si c'est le bon df à prendre.
    df_fisci_date_naissance = pd.merge(df_fisci, df_date_naissance, on='ID_FISC_LOG_DIFF')
    
    
    # In[98]:
    
    df_fisci_date_naissance[['ID_FISC_LOG_DIFF','Date_Naissance','Date_Naissance_Pere_fisci_y',
                                                                      'Date_Naissance_Mere_fisci_y']].head(9)
    
    
    # In[99]:
    
    print df_fisci_date_naissance.shape
    print df_fisci.shape
    
    
    # On a bien juste ajouté les deux dates de naissance à chaque logement.
    
    
    
    # In[100]:
    
    grpby = df_fisci_date_naissance.groupby('ID_FISC_LOG_DIFF').first().ID_DIFF.reset_index()
    
    
    # In[101]:
    
    merged = pd.merge(df_fisci_date_naissance,grpby, on ='ID_FISC_LOG_DIFF', how = 'left'); print merged.shape
    
    
    # In[102]:
    
    df_fisci.shape
    
    
    # In[103]:
    
    merged['ID_DIFF'] = merged['ID_DIFF_y']
    
    
    # In[104]:
    
    merged.shape
    
    
    # In[105]:
    
    merged['Id_fisc'] = merged['ID_FISC_FOY_DIFF'].astype('str')+ merged['ORDREFIP']+ merged['TYPE_FISC']
    
    
    # In[106]:
    
    merged.Id_fisc.drop_duplicates(inplace=True, take_last = True)
    
    
    # In[107]:
    
    merged.Id_fisc.unique().shape
    
    
    # On a bient tout le individus fiscaux qui ont un identifiant unique (meme shape)
    
    # In[108]:
    
    merged.shape
    
    
    # In[109]:
    
    merged.columns
    
    
    # In[110]:
    
    merged.groupby('ID_FISC_LOG_DIFF').sum().IS_IN_DESCENDANCE.value_counts()
    
    
    # 91446 logements fiscaux n'ayant pas d'évènement de naissance dans la table descendance.
    # 84908 ont un seul individu EDP, 2838 en ont deux. Ceux qui en ont trois ou 4 sont les enfants des parents qui ont eu des enfants.
    
    # On merge la table descendance contenant que les obs où tout les individus ont leur date de naissance renseignée.
    # 
    # 
    # df_select_2 : on garde que les ménages ou la date des parents et des enfants est renseigné dans l'état civil (fille de df_select ou on ne gardait que les déclarants EDP de fisci qui étaient dans DESCENDANCE 89000 individus.)
    
    # In[111]:
    
    df_descendance_select_2.columns
    
    
    # In[ ]:
    
    
    
    
    # In[112]:
    
    df_descend_fisci = df_descendance_select_2.merge(merged, on = 'ID_DIFF', how = 'right')
    
    
    # In[113]:
    
    merged.shape
    
    
    # In[114]:
    
    df_descend_fisci.shape
    
    
    # In[ ]:
    
    
    
    
    # In[115]:
    
    selected_columns = ['ID_FISC_LOG_DIFF', 'ID_DIFF', u'ENF_IND_NAI_DATE', 'ANAIS','JNAIS', 'Id_fisc']
    
    
    # In[116]:
    
    df_descend_fisci[selected_columns].sort(['ID_FISC_LOG_DIFF', 'ANAIS'])
    
    
    # In[117]:
    
    df_descend_fisci[(df_descend_fisci.ANAIS == df_descend_fisci.ENF_IND_NAI_DATE.str[0:4].astype('float'))][selected_columns].head()
    
    
    # In[118]:
    
    df_descend_fisci['Id_fisc'] = df_descend_fisci['ID_FISC_FOY_DIFF'].astype('str')+ df_descend_fisci['ORDREFIP']+ df_descend_fisci['TYPE_FISC']
    
    
    # In[119]:
    
    df_descend_fisci['Birth_date_matched'] = False
    
    
    # In[120]:
    
    selected_columns.extend(('Birth_date_matched','TYPE_FISC'))
    
    
    # In[121]:
    
    df_descend_fisci.TYPE_FISC.value_counts()
    
    
    # In[122]:
    
    (df_descend_fisci.ANAIS == df_descend_fisci.ENF_IND_NAI_DATE.str[0:4].astype('float')).value_counts()
    
    
    # In[123]:
    
    df_descend_fisci.loc[
        (df_descend_fisci.ANAIS == df_descend_fisci.ENF_IND_NAI_DATE.str[0:4].astype('float')), 'Birth_date_matched'
    ] = True
    
    
    # In[124]:
    
    df_descend_fisci.shape
    
    
    # In[125]:
    
    df_descend_fisci[df_descend_fisci.Birth_date_matched][selected_columns].groupby('ID_FISC_LOG_DIFF').count()['ANAIS'].value_counts(normalize = True)
    
    
    # In[126]:
    
    #(df_fisci.groupby('ID_FISC_LOG_DIFF').count()['ANAIS'] -2).value_counts(normalize = True)
    
    
    # Je compte et sélectionne (via l'index ID_FISC), les obseervations ou l'individu a l'année de naissance qui correspond.
    
    # In[127]:
    
    nb_matched_child = pd.DataFrame(
        df_descend_fisci[df_descend_fisci.Birth_date_matched][selected_columns].groupby('ID_FISC_LOG_DIFF').count()['ANAIS']
        )
    
    
    # In[128]:
    
    nb_matched_child.shape
    
    
    # In[129]:
    
    nb_child =  pd.DataFrame(
        df_fisci.groupby('ID_FISC_LOG_DIFF').count()['ANAIS']-2
        )
    
    
    # In[130]:
    
    compare_child_in_household = pd.merge(nb_matched_child, nb_child, left_index=True, right_index=True)
    
    
    # In[131]:
    
    compare_child_in_household.shape
    
    
    # Compare le nombre d'enfant dans le foyer.
    
    # In[132]:
    
    (compare_child_in_household['ANAIS_x'] == compare_child_in_household['ANAIS_y']).value_counts()
    
    
    # 59640 foyers ont le même nombre d'enfants que le nombre d'évenement naissance d'état civil avec la même année de naissance pour chaque enfant.
    
    # In[133]:
    
    keep_same_child_number_fisc_log_diff = compare_child_in_household[
        (compare_child_in_household['ANAIS_x'] == compare_child_in_household['ANAIS_y'])
        ].index
    
    
    # In[134]:
    
    df_descend_fisci_same_child_number = df_descend_fisci[
        df_descend_fisci.ID_FISC_LOG_DIFF.isin(keep_same_child_number_fisc_log_diff)
        ]
    
    
    # In[135]:
    
    df_descend_fisci_same_child_number.ID_FISC_LOG_DIFF
    
    
    # In[136]:
    
    df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').count().ANAIS.astype('bool').value_counts()
    
    
    # In[137]:
    
    selected_columns
    
    
    # In[ ]:
    
    
    
    
    # Mal fait les merge du coup on se retrouve avec des individus dupliqués, on les droppe un peu à la bourrin.
    
    # In[138]:
    
    
    df_descend_fisci_same_child_number = df_descend_fisci_same_child_number.drop_duplicates(["Id_fisc" ])
    
    
    # In[139]:
    
    
    df_descend_fisci_same_child_number[df_descend_fisci_same_child_number.Birth_date_matched][selected_columns].ID_FISC_LOG_DIFF.nunique()
    
    
    # In[140]:
    
    df_descend_fisci_same_child_number.shape
    
    
    # In[ ]:
    
    
    
    
    # #### Vérifie qu'on a bien le même nombre d'individus dans chaque logement fiscal 
    
    # In[141]:
    
    check_id_diff = df_descend_fisci_same_child_number.ID_FISC_LOG_DIFF
    
    
    # In[142]:
    
    check_fisci_number = df_fisci[df_fisci.ID_FISC_LOG_DIFF.isin(df_descend_fisci_same_child_number.ID_FISC_LOG_DIFF)
                   ].groupby('ID_FISC_LOG_DIFF').count().sort_index().ANAIS
    
    
    # In[143]:
    
    check_descend_fisci_same_child_number = df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').count().sort_index().ANAIS
    
    
    # In[144]:
    
    (check_fisci_number== check_descend_fisci_same_child_number).value_counts()
    
    
    # YES !!! Meme nombre d'individus dans tout les menages selectionnes.
    
    # In[145]:
    
    df_descend_fisci_same_child_number.sort('ID_FISC_LOG_DIFF')[selected_columns].head()
    
    
    # In[146]:
    
    df_descend_fisci_same_child_number['PERE_IND_NAI_DATE'] = pd.to_datetime(
        df_descend_fisci_same_child_number['PERE_IND_NAI_DATE'], infer_datetime_format=True, coerce=True
        )
    df_descend_fisci_same_child_number['MERE_IND_NAI_DATE'] = pd.to_datetime(
        df_descend_fisci_same_child_number['MERE_IND_NAI_DATE'], infer_datetime_format=True, coerce=True
        )
    
    
    # In[147]:
    
    pas_meme_date_pere = ~(df_descend_fisci_same_child_number.Date_Naissance_Pere_fisci_y == 
                          df_descend_fisci_same_child_number.PERE_IND_NAI_DATE)
    pas_meme_date_mere = ~(df_descend_fisci_same_child_number.Date_Naissance_Mere_fisci_y == df_descend_fisci_same_child_number.MERE_IND_NAI_DATE)
    
    
    # In[148]:
    
    df_descend_fisci_same_child_number.TYPE_FISC != '1'
    
    
    # In[149]:
    
    pas_meme_date_pere = (~(df_descend_fisci_same_child_number.Date_Naissance_Pere_fisci_y == 
                          df_descend_fisci_same_child_number.PERE_IND_NAI_DATE)&
                          (df_descend_fisci_same_child_number.TYPE_FISC != '1'))
    pas_meme_date_mere = (~(df_descend_fisci_same_child_number.Date_Naissance_Mere_fisci_y == 
                           df_descend_fisci_same_child_number.MERE_IND_NAI_DATE)&
                          (df_descend_fisci_same_child_number.TYPE_FISC != '1'))
    
    
    # In[150]:
    
    print pas_meme_date_pere.value_counts()
    print pas_meme_date_pere.value_counts(normalize=True)
    
    
    # In[151]:
    
    print pas_meme_date_mere.value_counts()
    print pas_meme_date_mere.value_counts(normalize=True)
    
    
    # In[152]:
    
    df_descend_fisci_same_child_number['Pas_meme_date_Pere'] = pas_meme_date_pere
    df_descend_fisci_same_child_number['Pas_meme_date_Mere'] = pas_meme_date_mere
    
    
    # In[153]:
    
    (df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').sum()['Pas_meme_date_Pere'].astype('bool')).value_counts()
    
    
    # In[154]:
    
    (
        (df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').sum()['Pas_meme_date_Pere'].astype('bool'))|
        (df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').sum()['Pas_meme_date_Mere'].astype('bool'))
      ).value_counts()
    
    
    # In[155]:
    
    (
        (df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').sum()['Pas_meme_date_Pere'].astype('bool'))|
        (df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').sum()['Pas_meme_date_Mere'].astype('bool'))
      ).value_counts(normalize = True)
    
    
    # Parmis les foyers sans union civile avec au moins un enfant mais dont aucun n'est en garde alternée, dont toutes les naissances de chaque enfant est renseignée dans l'état civil, on a  10% des foyers ont la mère ou le père qui n'est pas biologique.
    
    # In[156]:
    
    not_biologic_drop = (
        (df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').sum()['Pas_meme_date_Pere'].astype('bool'))|
        (df_descend_fisci_same_child_number.groupby('ID_FISC_LOG_DIFF').sum()['Pas_meme_date_Mere'].astype('bool'))
      )
    
    
    # In[157]:
    
    not_biologic_drop_id = not_biologic_drop[not_biologic_drop==True].index
    
    
    # In[158]:
    
    df_fisci_biologic = df_descend_fisci_same_child_number[~(df_descend_fisci_same_child_number.ID_FISC_LOG_DIFF.isin(not_biologic_drop_id))]
    
    
    # In[159]:
    
    df_descend_fisci_same_child_number.shape
    
    
    # In[160]:
    
    df_fisci_biologic.shape
    
    
    # In[161]:
    
    df_fisci_biologic.to_hdf('./Data/hdf/edp_concubin.h5', 
                             'Biologic_concubin_with_child_under_majority_rev_{}'.format(income_year))
    
    # In[ ]:
    
    import pickle
    path = (u"./Programme/pickle/optimize/Biologic/")
    
    pickle.dump(df_fisci_biologic.ID_FISC_LOG_DIFF.values, 
            open(path+"marriage_en_2013_2014.p", 'wb'))
    

if __name__ == '__main__':
#    for year in [2010]:
#        create_data_year(income_year = year)
    for year in range(2010,2015):
        create_data_year(income_year = year)
        print'year :{}'.format(year) *50
    