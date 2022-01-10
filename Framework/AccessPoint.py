from Dataframe import Dataframe
import pandas
from DataModification import *
import time

timestr = time.strftime("%Y%m%d-%H%M%S")

dati = Dataframe('RawDF.csv')
#dati = Dataframe()

df = getattr(dati, 'df')

df = fuzzyDgUniversity (df)
df = uniLocation(df)
df = fuzzyPT(df)
df, attribute = fuzzyLocation(df)
df = attrMobility(df, attribute)
df = fuzzyDg(df)
df = dottorato(df)
df = chronoPT(df)

df.to_csv('Clean-' +timestr+'.csv', encoding='utf-8-sig', index = False)
df.to_json('Clean-'+timestr+'.json')

print(df.head())



