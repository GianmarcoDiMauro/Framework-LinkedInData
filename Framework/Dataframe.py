import json
import pandas as pd
import os
from zipfile import ZipFile
import time
from faker import Faker
import io


class Dataframe:
    timestr = time.strftime("%Y%m%d-%H%M%S")
    fake = Faker()
    dictName = {}
    try:
        with io.open('dictName.txt', 'r', encoding='utf8') as f:
            file = f.readlines()
            for l in file :
                l = l.replace("\n", "")
                key, value = l.split(":")
                dictName[key] = value
    except:
        pass

    """
    def _sposta_file(self,timestr):
        with ZipFile(timestr+'.zip', 'w') as zipObj:
            for file in os.listdir('Dati Json'):
                if 'json' in file:
                    path = 'Dati Json/' + file
                    zipObj.write(path)
                    os.remove(path)
    """
        

    def _df_to_csv(self,df,timestr):
        df.to_csv(timestr+'.csv', encoding='utf-8-sig', index = False)
        

    def _elimina_doppi(self, df):
        df = df.loc[df.astype(str).drop_duplicates().index]
        df.reset_index(inplace=True, drop=True)
        return (df)

    def _inserisci_record(self, df):


        #variabile che conta i record
        index = len(df)
        #variabile che tiene traccia del prossimo numero di PT
        nPT= 1
        while ('PT' + str(nPT)) in df.columns:
            nPT +=1
        #variabile che tiene traccia del prossimo numero di Dg
        nDg= 1
        while ('Dg' + str(nDg)) in df.columns:
            nDg +=1


        #prendo ciclicamente il nome di ogni file nella cartella Dati Json
        for file in os.listdir('Dati Json'):
            #creo un path al file preso in considerazione dal ciclo
            path = 'Dati Json/' + file
            #lo leggo
            record = pd.read_json(path, typ="series", orient="record", convert_dates= False)
            #creo una riga vuota
            df.append(pd.Series(), ignore_index=True)
            #aggiungo i valori del file alla riga appena creata
            df.at[index,'Name'] = index
            Dataframe.dictName[index] = record['Name']


            try:
                df.at[index,'Location'] = record['Location']
            except:
                df.at[index,'Location'] = ""
    
            #Nel caso in cui il record ha dati per più occupazioni, creo altre colonne
            while len(record['Experiences']) >= nPT:
                df.insert(len(df.columns), 'PT'+str(nPT), '', False)
                df.insert(len(df.columns), 'PT'+str(nPT)+'Company', '', False)
                df.insert(len(df.columns), 'PT'+str(nPT)+'Duration', '', False)
                df.insert(len(df.columns), 'PT'+str(nPT)+'Location', '', False)
                df.insert(len(df.columns), 'PT'+str(nPT)+'FromDate', '', False)
                df.insert(len(df.columns), 'PT'+str(nPT)+'ToDate', '', False)
                df.insert(len(df.columns), 'PT'+str(nPT)+'Url', '', False)
                nPT+=1        
    
            #inserisco i valori nelle celle
            #Metto le professioni in ordine inverso da come ottenute da linkedin, così che PT1 sia il più vecchio, PT2 più recente
            for x in range (len(record['Experiences'])):
                df.at[index,'PT'+str(x+1)] = record['Experiences'][len(record['Experiences'])-(x+1)]['position_title']
                df.at[index,'PT'+str(x+1)+'Company'] = record['Experiences'][len(record['Experiences'])-(x+1)]['company']
                df.at[index,'PT'+str(x+1)+'Duration'] = record['Experiences'][len(record['Experiences'])-(x+1)]['duration']
                df.at[index,'PT'+str(x+1)+'Location'] = record['Experiences'][len(record['Experiences'])-(x+1)]['location']
                df.at[index,'PT'+str(x+1)+'FromDate'] = record['Experiences'][len(record['Experiences'])-(x+1)]['from_date']
                #Non ho idea del perché, ma su alcuni record mi dà l'errore "could not convert string to float" quando "aggiorno" il dataFrame, non quando lo creo.
                try:
                    df.at[index,'PT'+str(x+1)+'ToDate'] = record['Experiences'][len(record['Experiences'])-(x+1)]['to_date']
                except:
                    df.at[index,'PT'+str(x+1)+'ToDate'] = None
                    
                df.at[index,'PT'+str(x+1)+'Url'] = record['Experiences'][len(record['Experiences'])-(x+1)]['url']
        
            #Nel caso in cui il record ha dati per più studi accademici, creo altre colonne
            while len(record['Education']) >= nDg:
                df.insert(6+(4*(nDg-2)), 'Dg'+str(nDg), '', False)
                df.insert(7+(4*(nDg-2)), 'Dg'+str(nDg)+'University', '', False)
                df.insert(8+(4*(nDg-2)), 'Dg'+str(nDg)+'FromDate', '', False)
                df.insert(9+(4*(nDg-2)), 'Dg'+str(nDg)+'ToDate', '', False)
                nDg+=1   
    
            for x in range (len(record['Education'])):
                df.at[index,'Dg'+str(x+1)] = record['Education'][len(record['Education'])-(x+1)]['degree']
                df.at[index,'Dg'+str(x+1)+'University'] = record['Education'][len(record['Education'])-(x+1)]['university']
                df.at[index,'Dg'+str(x+1)+'FromDate'] = record['Education'][len(record['Education'])-(x+1)]['from_date']
                df.at[index,'Dg'+str(x+1)+'ToDate'] = record['Education'][len(record['Education'])-(x+1)]['to_date']
    
            index+=1

        df['id'] = df.index

        return (df)

    def _crea_DF(self, df):
        df = pd.DataFrame(columns=['Name','Location', 'Dg1', 'Dg1University', 'Dg1FromDate', 'Dg1ToDate', 'PT1', 'PT1Company', 'PT1Duration', 'PT1Location', 'PT1FromDate', 'PT1ToDate', 'PT1Url' ])
        self._inserisci_record(df)
        return (df)

        
    def __init__(self, file=None):
        self.file = file
        self.df = None
        flag = True

        if os.listdir('Dati Json'):
        
            if self.file == None:
                print('Creazione DataFrame')
                self.df = self._crea_DF(self.df)
                self.df = self._elimina_doppi(self.df)
                self._df_to_csv(self.df,self.timestr)
                #self._sposta_file(self.timestr)
            else :
                print('Aggiorno DataFrame')
                self.df =  pd.read_csv(self.file)
                self.df = self._inserisci_record(self.df)
                self.df = self._elimina_doppi(self.df)
                self._df_to_csv(self.df,self.timestr)
                #self._sposta_file(self.timestr)
        
        else:
            if self.file == None:
                print('Non sono presenti dati')
                flag = False
            else:
                try:
                    self.df = pd.read_csv(self.file)
                    flag = False
                except:
                    print('Il programma supporta solo file .csv')
        
        if flag:
            timestr = time.strftime("%Y%m%d-%H%M%S")
            with io.open("dictName"+"-"+ timestr+".txt", 'w', encoding='utf8') as f :
                for x, y in Dataframe.dictName.items():
                    f.write('%s:%s\n' % (x, y))
        



    