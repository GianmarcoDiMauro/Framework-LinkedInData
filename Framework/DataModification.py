import pandas as pd
import numpy as np
import string
import re
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import io
import time
from ast import literal_eval

#MIGLIORAMENTI: IL CODICE CICLA MOLTE VOLTE IL DATAFRAME RECORD PER RECORD. SI POTREBBE MODIFICARE IN MODO TALE CHE IL CICLO SIA SOLO UNO E CHE PER OGNI RECORD ESEGUA PIù OPERAZIONI. EX: IN UN SOLO CICLO POTREBBE ESEGUIRE FUZZYPT e FUZZYDG, se non ancora le altre. Ovviamente è una modifica che prevedrebbe un ripensamento e una parziale riscrittura del codice. Ex: tutte le creazioni di nuove colonne dovrebbero essere fatte anticipatamente. Ai metodi non dovrei passare l'intero dataframe, ma solo una riga. 


def _get_nDg(df):
    try :
        #conto il numero di colonne "dg" nel dataframe
        nDg = 1
        while ('Dg' + str(nDg)) in df.columns :
            nDg +=1
     
    except:
        print("Il dataframe potrebbe essere vuoto. ")
        quit
    
    return(nDg)

def _get_nPT(df):
    nPT = 1
    for x in enumerate (df.columns):
        if 'PT'+str(x[0]) in df.columns:
            nPT+=1
    return(nPT)


#normalizza la stringa s
def _normalization (s):
    s = s.lower().strip()
    for p in string.punctuation:
        s = s.replace(p, '')
    re.sub("\s\s+" , " ", s)
    return s

#standardizza tutte le scuole secondarie di secondo grado
def _scuoleSecondarie(s):
    lst = ['liceo ', 'itis ', 'istituto tecnico']
    if any(el in s for el in lst):
    # The any() function returns True if any item in an iterable are true, otherwise it returns False.
        s = "scuola secondaria di secondo grado"
    return s

#standardizzo manualmente le università toscane
def _uniTosc  (s):
    spl = s.split(' ')
    if any (el in spl for el in ['università', 'university', 'universitat', 'Universität', 'université', 'universidad']):
        if 'pisa' in spl:
            s = "Università di Pisa"
        elif 'siena' in spl:
            s = "Università degli Studi di Siena"
        elif any(el in spl for el in ['firenze', 'florence']):
            s = "Università degli Studi di Firenze"
        # else: pass
    return s

#legge file uni
def _readFile (array, nameFile):
    with io.open(nameFile, 'r', encoding='utf8') as f:
        file = f.readlines()
        for l in file :
            a = l.replace("\n", "")
            array.append(a)
    
    return (array)

def _readDictFile (dizionario, nomeFile):
    with io.open(nomeFile, 'r', encoding='utf8') as f:
        file = f.readlines()
        for l in file :
            l = l.replace("\n", "")
            key, value = l.split(":")
            dizionario[key] = value
    
    return (dizionario)

#aggiunge valori a uni
def _writeFileUni (CheckUni):
    
    oldCkeckUni = set()
    with io.open('uni.txt', 'r', encoding='utf8') as f:
        file = f.readlines()
        for l in file :
            a = l.replace("\n", "")
            oldCkeckUni.add(a)

    oldCkeckUni.update(CheckUni)
    timestr = time.strftime("%Y%m%d-%H%M%S")

    with io.open('uni-' + timestr +'.txt', 'w', encoding='utf8') as f :
        for x in oldCkeckUni:
            f.write(x)
            f.write('\n')

def _writeDictFIle (dizionario,nomeFile):
    timestr = time.strftime("%Y%m%d-%H%M%S")
    nF = nomeFile.split('.')
    with io.open(nF[0]+"-"+ timestr+".txt", 'w', encoding='utf8') as f :
        for x, y in dizionario.items():
            f.write('%s:%s\n' % (x, y))        

#sostituisce valori nelle colonne DgUniversity
def _sostituisciValoreDgUn (df, x, s):

    nDg = _get_nDg(df)
    for index in range (0, len(df)):
        for n in range (1, nDg):
            if df.at[index,'Dg' + str(n) +'University'] == x:
                df.at[index,'Dg' + str(n) +'University'] = s

#confronto i valori nelle colonne DgUniversity con il file DictUni
def _confrontoDictUni (df):
    nDg = _get_nDg(df)
    dictUni = {}
    nomeFile = 'dictUni.txt'
    _readDictFile(dictUni,nomeFile)

    for index in range (0, len(df)):
        for n in range (1, nDg):
            if df.at[index,'Dg' + str(n) +'University'] in dictUni:
                df.at[index,'Dg' + str(n) +'University'] = dictUni[df.at[index,'Dg' + str(n) +'University']]

#prende come valore un dataframe. Standardizza la uscite di DgUniversity
def fuzzyDgUniversity (df) :
    nDg = _get_nDg(df)
    uniFile = 'uni.txt'
    stateUniFile = 'StateUni.txt'
    listRemove = ['università', 'university', 'universitat', 'Universität', 'université', 'universidad', 'studi', 'libera', 'free' ]
    
    #normalizzo tutte le stringhe
    for index in range (0, len(df)):
        for num in range (1, nDg):
            if type(df.at[index,'Dg' + str(num) +'University']) is str:
                df.at[index,'Dg' + str(num) +'University'] = _normalization(df.at[index,'Dg'+ str(num) +'University'])
                df.at[index,'Dg' + str(num) +'University'] = _scuoleSecondarie(df.at[index,'Dg'+ str(num) +'University'])
                df.at[index,'Dg' + str(num) +'University'] = _uniTosc(df.at[index,'Dg'+ str(num) +'University'])

    #Prendo tutti i valori unici delle colonne DgUniversity
    Università = pd.DataFrame()
    for x in range (1, nDg):
        Università['Dg' + str(x) + 'University' ] = df['Dg' + str(x) + 'University' ]

    Università = Università.values.ravel()
    unique_value = pd.unique(Università)

    #creo le liste di check.
    stateUni = {}
    _readDictFile (stateUni, stateUniFile)
    CheckUni = []
    _readFile(CheckUni, uniFile)
    
    print('Procedere con la pulizia degli attributi "DgUniversity"?')
    print("Attenzione: l'operazione potrebbe richiedere molto tempo")
    while True:
        manual = input('Rispondere "s" o "n":')
        if manual.lower() == 's':
            break
        if manual.lower() == 'n':
            break
        else :
            print('Inserire valore valido \n')
    
    for x in unique_value :
        if isinstance (x, str):
            BestOne = process.extractOne(x, CheckUni)
    
            if BestOne[1]>= 93 :
                _sostituisciValoreDgUn(df, x, BestOne[0])
    
            elif  manual.lower() == 's':
                somm = 0
                for nb in range(0,nDg):
                    try:
                        somm += df['Dg' + str(nb) +'University'].value_counts()[x]
                    except:
                        continue
                if somm >= 10:
        
                    #prendo i 5 valori più simili e chiedo conferma
                    #Di default "extract" usa "Wratio" che però nel nostro caso non è ottimale
                    bestFive = process.extract(x, CheckUni, scorer= fuzz.ratio)
                            
                    #Flag che stoppa il processo quando troviamo una versione standardizzata della stringa
                    KeepGoing = True

                    for p in bestFive :
                        if KeepGoing :
                            if p[1]>=65:
                            
                                #faccio un check aggiuntivo, rimuovendo la parola "università" e facendo un ulteriore confronto.
                                stringaDF = x.split()
                                stringaProposta = p[0].lower().split()

                                stringaDF_NEW = [word for word in stringaDF if word not in listRemove]
                                stringaProposta_NEW = [word for word in stringaProposta if word not in listRemove]

                                final_stringaDF = ' '.join(stringaDF_NEW)
                                final_stringaProposta = ' '.join(stringaProposta_NEW)

                                simil = fuzz.ratio(final_stringaDF,final_stringaProposta )

                                if simil >= 50:
                                    print ('Valore "' + x +  '":')
                                    while True :
                                        print ('Si intende ' + str(p[0]) + '?')
                                        choice = input ('Rispondere "s" o "n":')
                                        print ("\n")
                                        choice = choice.lower().strip()

                                        if choice == 's' :
                                            _sostituisciValoreDgUn(df, x, p[0])

                                            KeepGoing = False
                                            break
                                    
                                        if choice == 'n' :
                                            break
                                    
                                        else :
                                            print ('Inserire valore valido \n')
                            
                #Se non abbiamo trovato nessuna soluzione valida, chiedo se vogliamo aggiungere il valore alla lista
                    if KeepGoing :
                    #chiedo di aggiugere solo se la stringa è presente almeno 5 volte in tutte il dataframe
                    
                        print ('Nessun valore simile trovato. Aggiungere "' + x + '" alla lista?')
                        print('ATTENZIONE: assicurarsi che non esista già il valore nella lista. Inserire un valore non corretto potrebbe portare problemi in futuro.')
                        choice = input ('Rispondere "s" o "n":')
                        print ("\n")

                        if choice == 's':
                            CheckUni.append(x)
                            print ('Università aggiunta \n')

                            #sta parte è sbaglaiata a merda, bisogna riscriverla
                            print ("Si vuole indicare la nazione dell'istituto? \n")

                            LocChoice = input ('Rispondere "s" se si acconsente:')
                            print ("\n")
                            LocChoice = LocChoice.lower()
                            if LocChoice == 's' :
                                inputNation = input ('Inserire nazione utilizzando la lingua inglese:')

                                #controllo se è una nazione

                                nation = []
                                _readFile(nation, 'nation.txt')

                                corrisp = 0
                                checkNation = process.extractOne(inputNation, nation)
                                corrisp = checkNation[1]

                                if corrisp >= 96:
                                    inputNation= checkNation[0] 
                                    stateUni[x] = inputNation

                                    print ('Località aggiunta correttamente')
                                    print("\n")
                                else:
                                    print('nessuna corrispondenza trovata')
                                    print("\n")
                            
                            else:
                                print('Località non aggiunta')
                                print("\n")

                        else:
                            print ('Università non aggiunta \n')
    
    _confrontoDictUni (df)

    #se l'utente ha scelto di modificare manualmente DgUniversity, salvo un nuovo file uni.txt
    if manual.lower() == 's':
        _writeFileUni (CheckUni)
        _writeDictFIle (stateUni, stateUniFile )
    
    return (df)

#Categorizza le varie uscite lavorative in 4 fasce 
def fuzzyPT (df) :
    nPT= _get_nPT(df)
    listaInternship = ['volunteer', 'volontario', 'stag', 'tirocin', 'trainee']
    listaPrimaF = ['consul', 'executive', 'contractor', 'associate', 'employee', 'teacher', 'lecturer', 'research', 'member' ]
    listaSecondaF = ['doctor', 'manager', 'coordinator', 'professor', 'respons' ]
    listaTerzaF = ['founder', 'ceo', 'owner','rector', 'chairman', 'president', 'chief', 'cfo', 'cmo', 'capo', 'admin', 'amministr', 'dirett', 'princiapal']
    errInt = [' international', 'internet']
    errCoo = ['coordinator', 'cooperation']
    errStud = ['phd', 'doctor', 'ph.d'  ]
    
    for index in range (0, len(df)):
        for n in range (1,nPT):
            if isinstance(df.at[index, 'PT'+str(n)],str):

                if 'intern' in df.at[index, 'PT'+str(n)].lower() and not any(el in df.at[index, 'PT'+str(n)].lower() for el in errInt) :
                    df.at[index, 'PT'+str(n)] = 'Internship'
                elif 'student' in df.at[index, 'PT'+str(n)].lower() and not any(el in df.at[index, 'PT'+str(n)].lower() for el in errStud) :
                    df.at[index, 'PT'+str(n)] = 'Internship'
                elif any(el in df.at[index, 'PT'+str(n)].lower() for el in listaInternship ):
                    df.at[index, 'PT'+str(n)] = 'Internship'

                elif  'coo' in df.at[index, 'PT'+str(n)].lower() and not any (el in df.at[index, 'PT'+str(n)].lower() for el in errCoo):
                    df.at[index, 'PT'+str(n)] = 'Terza Fascia'
                elif 'head' in df.at[index, 'PT'+str(n)].lower() and 'hunter' not in df.at[index, 'PT'+str(n)].lower():
                    df.at[index, 'PT'+str(n)] = 'Terza Fascia'
                elif any(el in df.at[index, 'PT'+str(n)].lower() for el in listaTerzaF ):
                    df.at[index, 'PT'+str(n)] = 'Terza Fascia'
                
                #faccio una prima selezione
                elif 'junior' in df.at[index, 'PT'+str(n)].lower() :
                    df.at[index, 'PT'+str(n)] = 'Prima Fascia'
                elif 'senior' in df.at[index, 'PT'+str(n)].lower() :
                    df.at[index, 'PT'+str(n)] = 'Seconda Fascia'

                elif 'post' in df.at[index, 'PT'+str(n)].lower() and 'doc' in df.at[index, 'PT'+str(n)].lower() :
                    df.at[index, 'PT'+str(n)] = 'Seconda Fascia'
                elif any(el in df.at[index, 'PT'+str(n)].lower() for el in listaSecondaF ):
                    df.at[index, 'PT'+str(n)] = 'Seconda Fascia'

                elif 'phd' in df.at[index, 'PT'+str(n)].lower() or 'ph.d' in df.at[index, 'PT'+str(n)].lower() :
                    df.at[index, 'PT'+str(n)] = 'PH.D'
                
                elif any(el in df.at[index, 'PT'+str(n)].lower() for el in listaPrimaF ):
                    df.at[index, 'PT'+str(n)] = 'Prima Fascia'

                elif df.at[index, 'PT'+str(n)] == '':
                    df.at[index, 'PT'+str(n)] = None
                else:
                    df.at[index, 'PT'+str(n)] = 'Prima Fascia'
    
    return (df)

#Crea due colonne per campo di studi e livello di studi          
def fuzzyDg (df) :
    DgMax= _get_nDg(df)
    listaHS = ['liceo', 'high school', 'istituto tecnico', 'itis ', 'maturità', 'scuola secondaria']
    listaBachelor = ['bachelor', 'laurea breve', 'triennale', 'primo livello', 'primo ciclo']
    listaMaster = ['master','laurea magistrale', 'laurea ciclo unico', 'secondo livello',  'secondo ciclo' ]
    listaPhd = ['ph.d', 'phd', 'dottorato', 'doctor of philosophy']

    listaLaw = ["law", "giurisprudenza", "giuridic", 'diritto', 'right'] 
    listaEconomics = ['econom', 'business', 'finanza', 'marketing', 'management', 'sviluppo sostenibile', 'finance', 'turismo', 'amministr']
    listaNatural = ['bio', 'chimica', 'geolog', 'chemistry', 'botan', 'fisiologia']
    listaSocial = [' pace', 'politic', 'policy', 'policies', 'social', 'sociolog', 'antropol', 'diplomatic', 'relations', 'relazioni internazionali']    
    listaAgrarian = ['natur', 'ambient', 'agrari', 'alimentari', 'food', 'enolog', 'agro', 'nutriz']  
    listaMath = ['matematica', 'math', 'fisica', 'physic']  
    listaInfo = ['informati', 'computer science', 'data science', 'cyber'] 
    listaVet = ['veterin', 'fauna']
    listaStory = ['story', 'filosofi', 'storia', 'archeo']
    listaLit = ['literature','letter', 'languag', 'lingu', 'filologia','philology', 'italianistica', 'beni culturali' ]
    listaHum = ['informatica umanistica', 'digital humanities','comunic', 'communic',' art ', ' arte ' 'spettacolo', 'moda', 'fashion', 'design', 'media', 'giornalismo', 'journalism',  'grafica','graphic', 'editoria'  ]
    listaEdu = ['formazione', 'educazione', 'education', 'pedagog' ]
    listaEng = ['ingegner','engineer', 'electr','robotic'  ]
    listaBigPharma = ['farmacia', 'farmac', 'pharma','erborist']
    listaHealthP = ['logopedia', 'speech therapy','fisioterapia', 'physiothe', 'infermier', 'nurs', 'radiolog', 'scienze motorie', 'exercise science'  ]
    listaMed = ['medicin','chirurgia','surgery','cardio', 'derma', 'neuro', 'anest', 'ematologia', 'oncolog', 'hematol']
    
    #creo le colonne vuote
    n = 2
    df.insert(n, 'High School', None, False)
    df.insert(n+1, "Bachelor's Degree", None, False)
    df.insert(n+2, "Master's Degree", None, False)
    df.insert(n+3, "Ph.D", None, False)
    df.insert(n+4, "Others", None, False)

    for index in range (0, len(df)):
        for y in range (1, DgMax):
            b = dict.fromkeys(['University','UniNation', 'Study Field', 'From Date', 'To Date', 'RawInfo'])
            dictFlagType = { 'b':"Bachelor's Degree", 'm':"Master's Degree", 'p':"Ph.D", 'o':"Others"}

            #compilo il dizionario
            if isinstance (df.at[index, 'Dg'+str(y)+'University'], str):
                b['University']= df.at[index, 'Dg'+str(y)+'University']
            if isinstance (df.at[index, 'Dg'+str(y)+'UniNation'], str):
                b['UniNation']= df.at[index, 'Dg'+str(y)+'UniNation']

            try:
                b['From Date'] = df.at[index, 'Dg'+str(y)+'FromDate']
            except:
                pass

            try:
                b['To Date'] = df.at[index, 'Dg'+str(y)+'ToDate']
            except:
                pass

            try:
                b['RawInfo'] = df.at[index, 'Dg'+str(y)]
            except:
                pass



            if isinstance (df.at[index, 'Dg'+str(y)],str) :
                df.at[index, 'Dg'+str(y)] = literal_eval(df.at[index, 'Dg'+str(y)])
            if isinstance (df.at[index, 'Dg'+str(y)],list):
                flagType = ''

                for posizione_Str in range (0, len(df.at[index, 'Dg'+str(y)])):
                    df.at[index, 'Dg'+str(y)][posizione_Str] = re.sub('"',"'",df.at[index,'Dg'+str(y)][posizione_Str])
                    df.at[index, 'Dg'+str(y)][posizione_Str] = re.sub('1°',"primo",df.at[index,'Dg'+str(y)][posizione_Str])
                    df.at[index, 'Dg'+str(y)][posizione_Str] = re.sub('2°',"secondo",df.at[index,'Dg'+str(y)][posizione_Str])
                    df.at[index, 'Dg'+str(y)][posizione_Str] = re.sub("dell'","dell ",df.at[index,'Dg'+str(y)][posizione_Str])
                    df.at[index, 'Dg'+str(y)][posizione_Str] = df.at[index,'Dg'+str(y)][posizione_Str].lower()

                stop = False
                for stringa in df.at[index, 'Dg'+str(y)]:
                    stop = False
                    if  any (el in stringa for el in listaHS):
                        if isinstance (df.at[index,'High School'], list):
                            df.at[index,'High School'].append(b)
                            stop = True
                            break
                        else:
                            a = []
                            a.append(b)
                            df.at[index,'High School'] = a
                            stop = True
                            break

                    elif any (el in stringa for el in listaBachelor):
                        if isinstance (df.at[index,"Bachelor's Degree"], list):
                            df.at[index,"Bachelor's Degree"].append(b)
                            flagType = 'b'
                            stop = True
                            break
                        else:
                            a = []
                            a.append(b)
                            df.at[index,"Bachelor's Degree"] = a
                            flagType = 'b'
                            stop = True
                            break

                    elif any (el in stringa for el in listaMaster):
                        if isinstance (df.at[index,"Master's Degree"], list):
                            df.at[index,"Master's Degree"].append(b)
                            flagType = 'm'
                            stop = True
                            break
                        else:
                            a = []
                            a.append(b)
                            df.at[index,"Master's Degree"] = a
                            flagType = 'm'
                            stop = True
                            break

                    elif any (el in stringa for el in listaPhd):
                        if isinstance (df.at[index,"Ph.D"], list):
                            df.at[index,"Ph.D"].append(b)
                            flagType = 'p'
                            stop = True
                            break
                        else:
                            a = []
                            a.append(b)
                            df.at[index,"Ph.D"] = a
                            flagType = 'p'
                            stop = True
                            break

                if not stop :
                    if isinstance (df.at[index,"Others"], list):
                        df.at[index,"Others"].append(b)
                        flagType = 'o'
                    else:
                        a = []
                        a.append(b)
                        df.at[index,"Others"] = a
                        flagType = 'o'


                if flagType != '':
                    for part in df.at[index, 'Dg'+str(y)]:
                        if any (el in part for el in listaLaw) :
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Law'
                            break
                        elif any (el in part for el in listaEconomics):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Economics'
                            break
                        elif any (el in part for el in listaSocial):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Social Science'
                            break
                        elif any (el in part for el in listaNatural):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Natural Science' 
                            break
                        elif any (el in part for el in listaMath):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Math and Physic'
                            break
                        elif any (el in part for el in listaHum):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Humanities'
                            break
                        elif any (el in part for el in listaInfo):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Informatics'
                            break
                        elif any (el in part for el in listaVet):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Veterinary Studies'
                            break
                        elif 'archite' in part:
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Architecture'
                            break
                        elif any (el in part for el in listaStory):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Story and Philosophy'
                            break
                        elif any (el in part for el in listaLit):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Literatures, Languages and Cultures'
                            break    
                        elif any (el in part for el in listaEdu):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Science Education'
                            break
                        elif any (el in part for el in listaEng):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Engineering'
                            break
                        elif 'psico'  in part or 'psycho' in part:
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Psicology'
                            break
                        elif any (el in part for el in listaBigPharma):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Pharmacology'
                            break
                        elif  any (el in part for el in listaMed):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Medicine'
                            break
                        elif any (el in part for el in listaHealthP):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Health Professions'
                            break
                        elif any (el in part for el in listaAgrarian):
                            df.at[index, dictFlagType[flagType]][-1]['Study Field'] = 'Agrarian Studies' 
                            break
    
    for x in range(1, DgMax):
        df.drop(['Dg'+str(x), 'Dg'+str(x)+'University', 'Dg'+str(x)+'UniNation', 'Dg'+str(x)+'FromDate', 'Dg'+str(x)+'ToDate'], axis=1, inplace = True)

    return (df)
    
def fuzzyLocation (df):
    #creo un array contenente le forme standardizzate delle nazioni
    locFile = 'nation.txt'
    nation = []
    _readFile(nation, locFile)

    #creo un dizionario, per cui ad ogni luogo corrisponde la nazione di riferimento
    city = {}
    nameCityFile = 'city.txt'
    _readDictFile(city, nameCityFile)
    
    #creo una lista con tutti gli attributi "location"
    attribute = ['Location']
    nPT = _get_nPT(df)
    for x in range (1, nPT):
        if 'PT'+str(x)+'Location' in df.columns:
            attribute.append('PT'+str(x)+'Location')

    for x in range (0, len(df)):
        for atr in attribute:
            if isinstance (df.at[x,atr], str) and df.at[x,atr] != 'None' and df.at[x,atr] != '' :
                #prendo l'ultima parte della stringa, in quanto molti valori seguono il pattern "città, regione, nazione"
                s = df.at[x,atr].split(',')
                df.at[x,atr] = s[-1].lstrip()

                #valuto se la stringa così ottenuta è una nazione, confontandola con il file "nation"
                isInNation = False
                for y in nation:
                    if df.at[x,atr] == y:
                        isInNation = True
                    
                #se non è una nazione, confronto con il dizionario "city" per vedere se vi è già assegnata una nazione
                if not isInNation:
                    if df.at[x,atr] in city:
                        df.at[x,atr] = city[df.at[x,atr]]
                
                    else:
                        #vedo se è una nazione scritta male
                        corrispondenza = 0
                        #Se prova[1] è uguale a 0, il programma ritorna una stringa vuota e il programma dà errore
                        try :
                            prova = process.extractOne(str(df.at[x,atr]), nation)
                            corrispondenza = prova[1]
                        except: 
                            continue
                        if corrispondenza >= 96:
                            df.at[x,atr] = prova[0]                    
                    
                        #chiedo all'utente se vuole inserire manualmente la nazione
                        elif atr == 'Location':
                        #Attualmente solo per "Location" in quanto le altre sono un po' troppo... casuali.
                            print ('\n')
                            print (df.at[x,atr])
                            print ('Si vuole indicare la nazione di riferimento? Rispondere "s" o "n"')
                            while True:
                                choice = input ("Attenzione: inserire il nome della nazione in inglese, con l'iniziale maiuscola. \n Se la nazione non corrisponde a nessuna nel file 'nation.txt' l'elemento non verrà aggiunto. : ")
                                print ('\n')
                
                                if choice.lower() == 's':
                                    ins = input ('Inserire nazione:')
                                    check = process.extractOne(ins, nation)
                    
                                    if check[1] >= 90:
                                        city [df.at[x,atr]] = check[0]
                                        df.at[x,atr] = check[0]
                                        break
                                    else :
                                        print ('La nazione non è stata riconosciuta. Il record non è stato modificato e non è stato aggiunto a "city.txt"')
                                        print ('\n')
                                        break
                
                                elif choice.lower() == 'n':
                                    df.at[x,atr] = None
                                    break
                                else :
                                    print ('Inserire valore valido')
                                    print ('\n')
                        else:
                            df.at[x,atr] = None
    
    _writeDictFIle (city,nameCityFile)                   
    return (df,attribute)

def uniLocation (df):
    # leggo file StateUni
    stateUni = {}
    stateUniFile = 'StateUni.txt'
    _readDictFile (stateUni, stateUniFile)
    
    nDg = _get_nDg(df)
    for x in range (1,nDg):
        n = df.columns.get_loc('Dg'+str(x)+'University')
        df.insert(n+1, 'Dg'+str(x)+'UniNation', '', False)
    
    for x in range (0, len(df)):
        for y in range (1,nDg):
            if df.at[x,'Dg'+str(y)+'University'] in stateUni:
                df.at[x,'Dg'+str(y)+'UniNation'] = stateUni[df.at[x,'Dg'+str(y)+'University']]
    
    return (df)

#Sposto i Ph.D da PT alla colonna apposita
def dottorato (df):
    nPT = _get_nPT(df)
    stateUni = {}
    stateUniFile = 'StateUni.txt'
    _readDictFile (stateUni, stateUniFile)

    for index in range (0, len(df)):
        for x in range (1, nPT):
            
            if df.at[index, 'PT'+str(x)] == 'PH.D':
                b = dict.fromkeys(['University','UniNation', 'Study Field', 'From Date', 'To Date', 'RawInfo'])
                b['University'] = df.at[index, 'PT'+str(x)+'Company']
                b['UniNation'] = df.at[index, 'PT'+str(x)+'Location']
                b['From Date'] = df.at[index, 'PT'+str(x)+'FromDate']
                b['To Date'] = df.at[index, 'PT'+str(x)+'ToDate']

                if b['University'] in stateUni:
                    b['UniNation']  = stateUni[b['University']]
            
                if isinstance (df.at[index, 'Ph.D'], list):
                    df.at[index, 'Ph.D'].append(b)
                else:
                    a = []
                    a.append(b)
                    df.at[index, 'Ph.D'] = a
                

                attrPTS = ['Company', 'Duration','Location', 'FromDate', 'ToDate','Url']
                df.at[index, 'PT'+str(x)] = None
                for attrPT in attrPTS:
                    df.at[index, 'PT'+str(x)+attrPT] = None
                
    
    return (df)

def chronoPT (df):
    PTMax = _get_nPT(df)
    attrPTS = ['Company', 'Duration','Location', 'FromDate', 'ToDate','Url']
    for index in range (0, len(df)): 
        
        if index == 1:
            print('Inizio ordinamento cronologico PT')
        
        if index%1000 == 0:
            print ('Ho elaborato ' +str(index) + ' record.')

        new_row = df.loc[index].copy()
        date = []
        attr = []
        for x in range (1, PTMax):
            try:
                date.append(int(df.at[index, 'PT'+str(x)+'FromDate']))
            except:
                date.append(None)
            attr.append('PT'+str(x)+'FromDate')
            
        s = pd.Series(date, attr)
        s = s.sort_values(ascending=True)
            
        for rif in range (0, len(s.index)):
            n = int(s.index[rif].split('T')[1].split('F')[0])
            new_row['PT'+str(rif+1)] = df.at[index, 'PT'+str(n)]
                
            for atr in attrPTS:
                new_row['PT'+str(rif+1)+atr] = df.at[index, 'PT'+str(n)+atr]
        
        
        df.loc[index] = new_row
    
    return (df)


#Creo l'attributo che sarà da riferimento per la classification
def attrMobility (df, attribute):
    nDg = _get_nDg(df)
    df.insert(len(df.columns), 'Mobility', '', False)
    
    for x in range (1, nDg):
        if 'Dg'+str(x) in df.columns:
            attribute.append('Dg'+str(x)+'UniNation')
    
    
    for n in range (0, len(df)):
        copyAttr = attribute.copy()

        for x in attribute:
            if isinstance (df.at[n,x],str):
                if df.at[n,x] == 'None' or df.at[n,x]== '' or df.at[n,x]== False:
                    copyAttr.remove(x)
            else:
                copyAttr.remove(x)
            
        flag = False
        for y in range(0,len(copyAttr)-1):
            if df.at[n,copyAttr[y]] != df.at[n,copyAttr[y+1]]:
                flag = True
                break
        
        if flag==True:
            df.at[n,'Mobility'] = 'y'
        else:
            df.at[n,'Mobility'] = 'n'
        
    return(df)